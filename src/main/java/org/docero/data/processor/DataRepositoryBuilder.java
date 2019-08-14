package org.docero.data.processor;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.DDataVersionalRepository;
import org.docero.data.DictionaryType;
import org.docero.data.remote.DDataPrototypeRealization;
import org.docero.data.utils.DDataDictionary;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class DataRepositoryBuilder {
    final String name;
    final TypeMirror forInterfaceName;
    final TypeMirror idClass;
    final TypeMirror actualTimeClass;
    final String daoClassName;
    final String restClassName;
    final TypeMirror repositoryInterface;
    final ArrayList<DDataMethodBuilder> methods = new ArrayList<>();
    final DDataBuilder rootBuilder;
    final String mappingClassName;
    final String repositoryVariableName;

    final DDataMethodBuilder defaultGetMethod;
    final DDataMethodBuilder defaultDeleteMethod;
    final DDataMethodBuilder defaultListMethod;
    final boolean hasInsert;
    final boolean hasUpdate;
    final HashMap<String, org.w3c.dom.Element> lazyLoads = new HashMap<>();
    final TypeMirror versionalType;
    final String[] beanImplementation;
    final DataRepositoryDiscriminator discriminator;

    DataRepositoryBuilder(
            DDataBuilder rootBuilder,
            TypeElement repositoryElement
    ) throws Exception {
        DDataRep ddRep = repositoryElement.getAnnotation(DDataRep.class);
        repositoryInterface = repositoryElement.asType();
        Types tu = rootBuilder.environment.getTypeUtils();
        this.mappingClassName = tu.erasure(repositoryInterface).toString();
        if (ddRep.value().length() > 0) name = ddRep.value();
        else name = mappingClassName.substring(mappingClassName.lastIndexOf('.'));
        this.rootBuilder = rootBuilder;

        boolean isHistorical = false;
        DeclaredType interfaceType = null;
        final String versionalType = DDataVersionalRepository.class.getCanonicalName();
        final String standartType = DDataRepository.class.getCanonicalName();
        final String dictionaryType = DDataDictionary.class.getCanonicalName();
        for (TypeMirror i : repositoryElement.getInterfaces()) {
            String iName = i.toString();
            isHistorical = iName.contains(versionalType);
            if (isHistorical || iName.contains(standartType) || iName.contains(dictionaryType))
                interfaceType = (DeclaredType) i;
            if (isHistorical) break;
        }
        if (interfaceType == null)
            throw new Exception("can't find interface for " + repositoryElement);

        String str = repositoryElement.getSimpleName().toString();
        repositoryVariableName = Character.toLowerCase(str.charAt(0)) + str.substring(1);

        List<? extends TypeMirror> gens = interfaceType.getTypeArguments();
        forInterfaceName = gens.get(0);
        idClass = gens.get(1);
        actualTimeClass = !isHistorical ? null : gens.get(2);
        DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());
        List<TypeMirror> forMultiplyInterfaces = rootBuilder.readBeansFromBeanElement(repositoryElement);
        if (bean == null || forMultiplyInterfaces.size() > 0) {
            beanImplementation = forMultiplyInterfaces.stream()
                    .map(t -> rootBuilder.beansByInterface.get(t.toString()))
                    .map(DataBeanBuilder::getImplementationName)
                    .toArray(String[]::new);
            TypeElement mte = rootBuilder.environment.getElementUtils().getTypeElement(forInterfaceName.toString());
            if (bean == null && mte != null && mte.getAnnotation(DDataPrototypeRealization.class) == null) {
                // if bean was not created by any extending beans (not single interface in declaration)
                bean = DataBeanBuilder.buildEntity(
                        mte,
                        rootBuilder,
                        rootBuilder.beansByInterface.get(forMultiplyInterfaces.get(0).toString()));
                rootBuilder.beansByInterface.put(forInterfaceName.toString(), bean);
                rootBuilder.unimplementedBeans.add(bean);
            }
            discriminator = new DataRepositoryDiscriminator(
                    forMultiplyInterfaces.stream()
                            .map(t -> rootBuilder.beansByInterface.get(t.toString()))
                            .collect(Collectors.toList())
            );
        } else {
            beanImplementation = new String[]{bean.getImplementationName()};
            discriminator = null;
        }
        this.versionalType = !isHistorical ? null : gens.get(2);
        daoClassName = repositoryElement.asType().toString() + "_Dao_";
        restClassName = repositoryElement.asType().toString() + "Controller";

        for (Element element : repositoryElement.getEnclosedElements())
            if (element.getKind() == ElementKind.METHOD) {
                if (!element.getModifiers().contains(Modifier.STATIC) ?
                        !element.getModifiers().contains(Modifier.DEFAULT) :
                        element.getModifiers().contains(Modifier.ABSTRACT)) {
                    methods.add(new DDataMethodBuilder(this, (ExecutableElement) element));
                }
            }

        defaultGetMethod = methods.stream().filter(m ->
                "get".equals(m.methodName) && m.parameters.size() == 1 &&
                        tu.isSameType(m.parameters.get(0).type, idClass)
        ).findAny().orElse(null);

        defaultDeleteMethod = methods.stream().filter(m ->
                "delete".equals(m.methodName) && m.parameters.size() == 1 &&
                        tu.isSameType(m.parameters.get(0).type, idClass)
        ).findAny().orElse(null);

        defaultListMethod = methods.stream().filter(m ->
                "list".equals(m.methodName) && m.parameters.size() == 0
        ).findAny().orElse(null);

        hasInsert = methods.stream().anyMatch(m -> "insert".equals(m.methodName));
        hasUpdate = methods.stream().anyMatch(m -> "update".equals(m.methodName));
    }

    private DataRepositoryBuilder(DDataBuilder rootBuilder, DataBeanBuilder bean) {
        this.forInterfaceName = bean.interfaceType;
        this.idClass = rootBuilder.environment.getElementUtils().getTypeElement(bean.inversionalKey).asType();
        this.actualTimeClass = bean.versionalType;
        this.daoClassName = bean.interfaceType + "_Dao_";
        this.restClassName = null;
        Types tu = rootBuilder.environment.getTypeUtils();
        this.mappingClassName = tu.erasure(forInterfaceName).toString();
        String str = mappingClassName.substring(mappingClassName.lastIndexOf('.') + 1) + "Repository";
        repositoryVariableName = Character.toLowerCase(str.charAt(0)) + str.substring(1);
        this.versionalType = bean.versionalType;
        this.repositoryInterface = versionalType != null ?
                tu.getDeclaredType(
                        rootBuilder.environment.getElementUtils().getTypeElement("org.docero.data.DDataVersionalRepository"),
                        forInterfaceName, idClass, versionalType) :
                tu.getDeclaredType(
                        rootBuilder.environment.getElementUtils().getTypeElement("org.docero.data.DDataRepository"),
                        forInterfaceName, idClass);
        this.name = mappingClassName.substring(mappingClassName.lastIndexOf('.'));
        this.beanImplementation = new String[]{bean.getImplementationName()};
        discriminator = null;
        //final ArrayList<DDataMethodBuilder> methods = new ArrayList<>();
        this.rootBuilder = rootBuilder;
        defaultGetMethod = null;
        defaultDeleteMethod = null;
        defaultListMethod = null;
        hasUpdate = false;
        hasInsert = false;
    }

    String forInterfaceName() {
        return forInterfaceName == null ?
                "invalid." + name :
                forInterfaceName.toString();
    }

    void buildAnnotations(ProcessingEnvironment environment, boolean spring) throws IOException {
        buildFilterAnnotation(environment);
        buildDataFetchAnnotation(environment);
    }

    private boolean isAlreadyBuild = false;

    void build(ProcessingEnvironment environment, boolean spring) {
        if (isAlreadyBuild) return;
        isAlreadyBuild = true;

        int simpNameDel = daoClassName.lastIndexOf('.');

        try (JavaClassWriter cf = new JavaClassWriter(environment, daoClassName)) {
            String beanPkg = forInterfaceName.toString();
            beanPkg = beanPkg.substring(0, beanPkg.lastIndexOf('.'));
            cf.println("package " +
                    daoClassName.substring(0, simpNameDel) + ";");

            if (!daoClassName.substring(0, daoClassName.lastIndexOf(".")).equals(beanPkg))
                cf.println("import " + beanPkg + ".*;");

            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");

            DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());

            rootBuilder.checkAbstractRepositoryForPackage(beanPkg);
            cf.startBlock("public final class " +
                            daoClassName.substring(simpNameDel + 1) +
                    " extends " + rootBuilder.basePackage + ".AbstractModuleRepository<" +
                            bean.interfaceType + "," + bean.inversionalKey + ">" +
                            " implements " + repositoryInterface + (bean.isDictionary() ?
                            ", org.docero.data.utils.DDataDictionary<" +
                                    bean.interfaceType + "," + bean.inversionalKey + ">" :
                            ""
                    ) + " {"
            );

            if (!spring) {
                //TODO without Spring
                cf.startBlock("private org.apache.ibatis.session.SqlSession getSqlSession() {");
                cf.println("return null;");
                cf.endBlock("}");
            }

            if (bean.dictionary != DictionaryType.NO) {
                cf.println("");
                cf.startBlock("public org.docero.data.DictionaryType getDictionaryType() {");
                cf.println("return org.docero.data.DictionaryType." + bean.dictionary + ";");
                cf.endBlock("}");
                cf.println("");
                cf.startBlock("public Class<" + bean.interfaceType + "> getItemInterface() {");
                cf.println("return " + bean.interfaceType + ".class;");
                cf.endBlock("}");
                cf.println("");
                if (spring) cf.println("@org.springframework.cache.annotation.CachePut(cacheNames=\"" +
                            bean.cacheMap + "\", key = \"#bean.dDataBeanKey_\")");
                cf.startBlock("public <T extends " + forInterfaceName + "> T put_(T bean) {");
                        //TODO without Spring
                cf.println("return bean;");
                cf.endBlock("}");
                cf.println("");
                if (spring)
                    cf.println("@org.springframework.cache.annotation.Cacheable(cacheNames=\"ddata.dictionaries\", key = \"'" +
                            bean.cacheMap + "'\")");
                cf.startBlock("public Integer version_() {");
                        //TODO without Spring
                cf.println("return 0;");
                cf.endBlock("}");
                if (spring)
                    cf.println("@org.springframework.cache.annotation.CachePut(cacheNames=\"ddata.dictionaries\", key = \"'" +
                            bean.cacheMap + "'\")");
                cf.startBlock("public Integer version_(Integer i) {");
                        //TODO without Spring
                cf.println("return i;");
                cf.endBlock("}");
            }
            mapOfAttributesBuilder(cf, bean);
            buildMethodCreate(cf);

            TypeElement keyElement = rootBuilder.environment.getElementUtils().getTypeElement(bean.keyType);

            if (defaultGetMethod == null) {
                if (keyElement != null)
                    new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.GET, keyElement).build(cf);
                else cf.println("public " + bean.interfaceType + " get(" + bean.keyType + " v) {}");
                //buildMethodGet(cf);
            }
            if (!hasInsert) {
                new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.INSERT, keyElement).build(cf);
                //buildMethodInsert(cf, discriminator);
            }
            if (!hasUpdate) {
                new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.UPDATE, keyElement).build(cf);
                //buildMethodUpdate(cf, discriminator);
            }
            if (defaultDeleteMethod == null) {
                if (keyElement != null)
                    new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.DELETE, keyElement).build(cf);
                else cf.println("public void delete(" + bean.keyType + " v) {}");
                //buildMethodDelete(cf);
            }

            if (bean.dictionary != DictionaryType.NO && defaultListMethod == null) {
                cf.println("");
                cf.startBlock("public java.util.List<" + forInterfaceName + "> list() {");
                if (bean.dictionary == DictionaryType.SMALL)
                    cf.println("return listCached(" + forInterfaceName + ".class,getSqlSession(),\"" +
                            mappingClassName + ".dictionary\");");
                else
                    cf.println("return getSqlSession().selectList(\"" +
                            mappingClassName + ".dictionary\");");
                cf.endBlock("}");
            }

            if (!bean.isDictionary()) {
                buildDocToSaveMethod(cf);
                buildMethodSave(bean, cf);
            } else {
                cf.startBlock("public " + bean.inversionalKey + " save(");
                cf.println(bean.interfaceType + " bean,");
                cf.println("org.docero.data.utils.UpdateOptions updateOptions,");
                cf.println("org.docero.data.DData dData,");
                cf.println("java.util.Set savedBeans");
                cf.endBlock(")");
                cf.startBlock("{");
                cf.println("if(bean != null) updateOptions.handledBean(bean);");
                cf.println("return null;");
                cf.endBlock("}");
            }
            for (DDataMethodBuilder method : methods)
                method.build(cf);
            cf.println("}");
        } catch (Exception e) {
            throw new RuntimeException("Can't build repository for " + forInterfaceName.toString(), e);
        }
    }

    private void buildFilterAnnotation(ProcessingEnvironment environment) throws IOException {
        int simpNameDel = daoClassName.lastIndexOf('.');
        try (JavaClassWriter cf = new JavaClassWriter(environment, mappingClassName + "_Filter_")) {
            final String annotName = mappingClassName.substring(mappingClassName.lastIndexOf('.') + 1) + "_Filter_";
            cf.println("package " +
                    daoClassName.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.println("import org.docero.data.DDataFilterOption;");
            cf.println("");
            cf.println("import java.lang.annotation.ElementType;");
            cf.println("import java.lang.annotation.Retention;");
            cf.println("import java.lang.annotation.RetentionPolicy;");
            cf.println("import java.lang.annotation.Target;");
            cf.println("@Retention(RetentionPolicy.SOURCE)");
            cf.println("@Target(ElementType.PARAMETER)");
            cf.startBlock("@interface " + annotName + " {");
            cf.println(forInterfaceName + "_ value() default " +
                    forInterfaceName + "_.NONE_;");
            cf.println("DDataFilterOption option() default DDataFilterOption.EQUALS;");
            DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());
            for (DataBeanPropertyBuilder property : bean.properties.values()) {
                TypeMirror typeErasure = environment.getTypeUtils().erasure(property.isCollection() ?
                        ((DeclaredType) property.type).getTypeArguments().get(0) : property.type);
                DataBeanBuilder manType = rootBuilder.beansByInterface.get(typeErasure.toString());
                if (manType != null) {
                    cf.println(manType.interfaceType + "_ " +
                            property.name + "() default " +
                            manType.interfaceType + "_.NONE_;");
                }
            }
            cf.endBlock("}");
        }
    }

    private void buildDataFetchAnnotation(ProcessingEnvironment environment) throws IOException {
        int simpNameDel = daoClassName.lastIndexOf('.');
        try (JavaClassWriter cf = new JavaClassWriter(environment, mappingClassName + "_DDataFetch_")) {
            final String annotName = mappingClassName.substring(mappingClassName.lastIndexOf('.') + 1) + "_DDataFetch_";
            cf.println("package " +
                    daoClassName.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.println("import org.docero.data.DDataFetchType;");
            cf.println("");
            cf.println("import java.lang.annotation.ElementType;");
            cf.println("import java.lang.annotation.Retention;");
            cf.println("import java.lang.annotation.RetentionPolicy;");
            cf.println("import java.lang.annotation.Target;");
            cf.println("@Retention(RetentionPolicy.SOURCE)");
            cf.println("@Target({ElementType.METHOD,ElementType.FIELD})");
            cf.startBlock("@interface " + annotName + " {");

            cf.startBlock("/**");
            cf.println("Load mapped entities in single select<br>");
            cf.println("DDataFetchType.NO - truncate any association and collection attributes from loads " +
                    "(like a eagerTrunkLevel=0 with truncateMapped=true)<br>");
            cf.println("DDataFetchType.LAZY - do lazy loads <br>");
            cf.println("DDataFetchType.EAGER - do eager loads up to eagerTrunkLevel<br>");
            cf.println("DDataFetchType.COLLECTIONS_ARE_LAZY - do eager load for mappedElements and lazy for collections<br>");
            cf.println("default DDataFetchType.COLLECTIONS_ARE_LAZY<br>");
            cf.println("@return load type of mapped entities");
            cf.endBlock("*/");
            cf.println("DDataFetchType value() default DDataFetchType.COLLECTIONS_ARE_LAZY;");

            cf.startBlock("/**");
            cf.println("Fields what being not loaded (inserted and updated) at all, nor EAGER, nor LAZY");
            cf.println("@return ignored fields");
            cf.endBlock("*/");
            cf.println(forInterfaceName + "_[] ignore() default " + forInterfaceName + "_.NONE_;");

            cf.startBlock("/**");
            cf.println("Fields what only being loaded, inserted and updated. If set, 'ignore' property being not used");
            cf.println("@return exclusively fields");
            cf.endBlock("*/");
            cf.println(forInterfaceName + "_[] exclusively() default " + forInterfaceName + "_.NONE_;");

            cf.startBlock("/**");
            cf.println("Level for EAGER loaded beans mappedElements and collections attributes<br>");
            cf.println("0 - load all lazy<br>");
            cf.println("1 - provide eager load of mappedElements and collections attributes in first level of eager loaded beans, " +
                    "and lazy for 2-nd level<br>");
            cf.println("default 1");
            cf.println("@return eager loads truncation");
            cf.endBlock("*/");
            cf.println("int eagerTrunkLevel() default 1;");

            cf.startBlock("/**");
            cf.println("Truncate LAZY loaded beans mappedElements and collections attributes<br>");
            cf.println("default false");
            cf.println("@return do truncation for mapped beans from eagerTrunkLevel");
            cf.endBlock("*/");
            cf.println("boolean truncateLazy() default false;");

            cf.println(forInterfaceName + "_[] forwardOrder() default {};");
            cf.println(forInterfaceName + "_[] backwardOrder() default {};");

            cf.endBlock("}");
        }
    }

    static DataRepositoryBuilder build(
            DDataBuilder rootBuilder,
            DataBeanBuilder bean
    ) throws IOException {
        return new DataRepositoryBuilder(rootBuilder, bean);
    }

    private void buildMethodCreate(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public " + forInterfaceName + " create() {");
        cf.println("return new " + beanImplementation[0] + "();");
        cf.endBlock("}");
    }

    void onMethod(ExecutableElement method, Consumer<? super DDataMethodBuilder> consumer) {
        Types tu = this.rootBuilder.environment.getTypeUtils();
        String returntype = method.getReturnType() != null ? method.getReturnType().toString() : "";
        String paramHash = method.getParameters().stream()
                .map(e -> tu.erasure(e.asType()))
                .map(TypeMirror::toString)
                .collect(Collectors.joining(","));
        methods.stream().filter(m -> method.getSimpleName().toString().equals(m.methodName))
                .filter(m -> m.returnType != null && returntype.equals(m.returnType.toString()))
                .filter(m -> m.parameters.size() == method.getParameters().size())
                .filter(m -> m.parameters.stream().map(p -> tu.erasure(p.type))
                        .map(TypeMirror::toString)
                        .collect(Collectors.joining(",")).equals(paramHash))
                .findAny().ifPresent(consumer);
    }

    private void buildMethodSave(DataBeanBuilder bean, JavaClassWriter cf) throws IOException {
        cf.println("public " + bean.inversionalKey + " save(" + bean.interfaceType + " bean,\n" +
                "org.docero.data.utils.UpdateOptions updateOptions,\n" +
                "org.docero.data.DData dData,\n" +
                "java.util.Set savedBeans) {\n" +
                "if (savedBeans.contains(bean))\n" +
                "            return getKey(bean);\n" +
                "savedBeans.add(bean);\n" +
                "updateOptions.handledBean(bean);\n" +
                "java.util.Set<org.docero.data.utils.DDataAttribute> included" +
                " = updateOptions.generatedAttributes(attributes,attributesMarkedJsonIgnore,attributesMarkedXmlTransient);\n");

        if (bean.versionalType != null && bean.properties.values().stream().filter(s -> s.isId && !s.isVersionFrom).count() == 1) {
            DataBeanPropertyBuilder dataBeanPropertyBuilder =
                    bean.properties.values().stream().filter(s -> s.isId && !s.isVersionFrom).findAny().get();
            cf.println(bean.interfaceType + " beanFromDB = get(bean.get" + getCapitalizeName(dataBeanPropertyBuilder.name) + "());\n");
        } else
            cf.println(bean.interfaceType + " beanFromDB = get(getKey(bean));\n");


        cf.println(bean.inversionalKey + " returnedKey = null;");
        cf.println("        if ( beanFromDB != null){");
        if (beanImplementation.length > 1) {
            for (String beanImpl : beanImplementation) {
                cf.println("if( bean instanceof " + beanImpl + "){");
                changeFieldOfBeanForUpdate(bean, cf, beanImpl);
                cf.println("}");
            }
        } else
            changeFieldOfBeanForUpdate(bean, cf, beanImplementation[0]);

        cf.println("if(((" + rootBuilder.basePackage + ".AbstractBean)beanFromDB).compareSimpleTypes(bean) != 0) \n" +
                "update(bean);\n" +
                "}else{");

        if (beanImplementation.length > 1) {
            for (String beanImpl : beanImplementation) {
                cf.println("if( bean instanceof " + beanImpl + "){");
                changeFieldOfBeanForInsert(bean, cf, beanImpl);
                cf.println("}");
            }
        } else
            changeFieldOfBeanForInsert(bean, cf, beanImplementation[0]);

        cf.println("            }\n" +
                "     try {\n" +
                "            getSqlSession().getConnection().commit();\n" +
                "        } catch (java.sql.SQLException e) {\n" +
                "            e.printStackTrace();\n" +
                "        }\n" +
                "       return returnedKey;}");
    }

    private void changeFieldOfBeanForInsert(DataBeanBuilder bean, JavaClassWriter cf, String beanImpl) throws IOException {
        cf.println(bean.interfaceType + " beanGetting = insert(bean);\n" +
                "returnedKey = getKey(beanGetting);");
        settingNewIdForBean(bean, cf, beanImpl);
        settingOnlyMappingBeans(bean, cf, beanImpl);
        settingMappedAndMappingBeans(bean, cf, beanImpl);
        cf.println("update(bean);");
    }

    private void settingOnlyMappingBeans(DataBeanBuilder bean, JavaClassWriter cf, String beanImpl) throws IOException {
        for (DataBeanPropertyBuilder property : bean.properties.values()) {
            DataBeanBuilder mappedBean = rootBuilder.beansByInterface.get(property.mappedType.toString());
            Mapping mapping = rootBuilder.mappings.get(property.dataBean.interfaceType + "." + property.name);
            Mapping mappingInvert = rootBuilder.mappings.get(property.mappedType.toString() + "." + bean.name);
            String capitalizeName = getCapitalizeName(property.name);
            if ((mappedBean == null && mapping != null)
                    || isPropertyAreDictionary(property)
                    || mappingInvert != null) {
                continue;
            }

            if ( isMapingIdOnId(bean, property) && !property.isCollection()) {
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                cf.println("if(((" + beanImpl + ")bean).get" + capitalizeName + "() != null){\n" +
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);");
                cf.println("dao.save(((" + beanImpl + ")bean).get" + capitalizeName + "(),updateOptions,dData,savedBeans);");
                cf.println("}");
            }

            if (isManyToSingle(bean, property) && !isMapingIdOnId(bean, property) && !property.isCollection()) {
                String[] mappedProps = mapping.mappedProperties.stream().map(s -> s.name).toArray(String[]::new);
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                String[] mappingProps = mapping.properties.stream().map(s -> s.name).toArray(String[]::new);
                cf.println("if(((" + beanImpl + ")bean).get" + capitalizeName + "() != null){\n" +
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);");
                cf.println(rootBuilder.beansByInterface.get(property.type.toString()).inversionalKey
                        + " savedKey = dao.save(((" + beanImpl + ")bean).get" + capitalizeName + "()," +
                        "updateOptions,dData,savedBeans);\n" +
                        "if(savedKey != null){\n" +
                        property.mappedType + " savedInnerBean = dao.get(savedKey);");
                for (int i = 0; i < mappedProps.length; i++)
                    cf.println("((" + beanImpl + ")bean).set"
                            + getCapitalizeName(mappingProps[i]) + "(savedInnerBean.get" + getCapitalizeName(mappedProps[i]) + "());");

                cf.println("}\n}");
            }
            if (mapping != null && property.isCollection()) {
                String[] mappedProps = mapping.mappedProperties.stream().map(s -> s.name).toArray(String[]::new);
                String[] mappingProps = mapping.properties.stream().map(s -> s.name).toArray(String[]::new);
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                cf.println("if(((" + beanImpl + ")bean).get" + capitalizeName + "() != null){\n" +
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);\n" +
                        "for(" + property.mappedType + " b : bean.get" + capitalizeName + "()){\n");
                if (mappedProps.length > 1)
                    for (int i = 0; i < mappedProps.length; i++) {
                        cf.println("b.set" + getCapitalizeName(mappedProps[i]) + "(bean.get" + getCapitalizeName(mappingProps[i]) + "());");
                    }
                else
                    cf.println("b.set" + getCapitalizeName(mappedProps[0]) + "(bean.get" + getCapitalizeName(mappingProps[0]) + "());");

                cf.println("dao.save(b,updateOptions,dData,savedBeans);");
                cf.println("}\n}");
            }
        }
    }

    private void settingMappedAndMappingBeans(DataBeanBuilder bean, JavaClassWriter cf, String beanImpl) throws
            IOException {
        for (DataBeanPropertyBuilder property : bean.properties.values()) {
            String capitalizeName = getCapitalizeName(property.name);
            DataBeanBuilder mappedBean = rootBuilder.beansByInterface.get(property.mappedType.toString());
            Mapping mappingInvert = rootBuilder.mappings.get(property.mappedType.toString() + "." + bean.name);
            if (mappingInvert == null)
                continue;
            String[] mappedProps = mappingInvert.mappedProperties.stream().map(s -> s.name).toArray(String[]::new);
            String[] mappingProps = mappingInvert.properties.stream().map(s -> s.name).toArray(String[]::new);
            if (!isMapingIdOnId(bean, property) && property.isCollection()) {
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                cf.println("if(bean.get" + getCapitalizeName(property.name) + "() != null){\n" +
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);" +
                        "for(" + property.mappedType + " b : bean.get" + getCapitalizeName(property.name) + "()){\n");
                for (int i = 0; i < mappedProps.length; i++) {
                    cf.println("b.set" + getCapitalizeName(mappingProps[i]) +
                            "(bean.get" + getCapitalizeName(mappedProps[i]) + "());");
                }
                cf.println("dao.save(b, updateOptions,dData,savedBeans);\n" +
                        "}}");
                continue;
            }
            if (!isMapingIdOnId(bean, property) && mappedBean != null && !property.isCollection()) {
                String inversionalKey = rootBuilder.beansByInterface.get(property.mappedType.toString()).inversionalKey;
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                cf.println("if(((" + beanImpl + ") bean).get" + capitalizeName + "() != null &&" +
                        " !savedBeans.contains(((" + beanImpl + ") bean).get" + capitalizeName + "())){\n");
                for (int i = 0; i < mappedProps.length; i++)
                    cf.println("((" + beanImpl + ")bean).get" + getCapitalizeName(property.name) + "().set" + getCapitalizeName(mappingProps[i]) + "(((" +
                            beanImpl + ")bean).get" + getCapitalizeName(mappedProps[i]) + "());");
                cf.println(
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);\n" +
                                inversionalKey + " key = dao.save(bean.get" + capitalizeName + "()," +
                                " updateOptions,dData,savedBeans);"
                );
                mappingProps = rootBuilder.mappings.get(bean.interfaceType + "." + property.name).properties
                        .stream().map(s -> s.name).toArray(String[]::new);
                if (mappedProps.length > 1) {
                    for (int i = 0; i < mappedProps.length; i++) {
                        cf.println("((" + beanImpl + ")bean).set" + getCapitalizeName(mappingProps[i]) + "(key.get" + getCapitalizeName(mappedProps[i]) + "());");
                    }
                } else {
                    cf.println("((" + beanImpl + ")bean).set" + getCapitalizeName(mappingProps[0]) + "(key);");
                }
                cf.println("}");

            }
        }
    }

    private boolean isPropertyAreDictionary(DataBeanPropertyBuilder property) {
        return rootBuilder.beansByInterface.get(property.type.toString()) != null
                && rootBuilder.beansByInterface.get(property.type.toString()).isDictionary();
    }

    /**
     * else id of bean generated from sequins of DB then need setup generate id
     */
    private void settingNewIdForBean(DataBeanBuilder bean, JavaClassWriter cf, String beanImpl) throws IOException {
        for (DataBeanPropertyBuilder property : bean.properties.values()) {
            String capitalizeName = getCapitalizeName(property.name);
            if (property.isId())
                cf.println("if(((" + beanImpl + ") bean).get" + capitalizeName + "() != " +
                        "((" + beanImpl + ") beanGetting).get" + capitalizeName + "())\n" +
                        "((" + beanImpl + ") bean).set" + capitalizeName + "(((" + beanImpl + ") beanGetting).get" + capitalizeName + "());");
        }
    }

    private void buildDocToSaveMethod(JavaClassWriter cf) throws IOException {
        cf.println("/**\n" +
                "     * it is intended to update a bean already present in the database\n" +
                "     * and insert if the bean is not in the database\n" +
                "     * @param v is saved bean\n" +
                "     * @param updateOptions for inclusion, exclusion of attributes and processing of the bean before saving\n" +
                "     * @param dData gives access to repositories for recursive storage\n" +
                "     * @return key of saved bean\n" +
                "     */");
    }

    private void changeFieldOfBeanForUpdate(
            DataBeanBuilder bean,
            JavaClassWriter cf,
            String beanImpl) throws IOException {
        for (DataBeanPropertyBuilder property : bean.properties.values()) {
            DataBeanBuilder mappedBean = rootBuilder.beansByInterface.get(property.mappedType.toString());
            Mapping mapping = rootBuilder.mappings.get(property.dataBean.interfaceType + "." + property.name);
            String capitalizeName = getCapitalizeName(property.name);
            if ((mappedBean == null && mapping != null)) {
                continue;
            }
            cf.println("if(!included.contains(" + forInterfaceName() + "_WB_." + property.enumName + "))\n" +
                    "   ((" + beanImpl + ")bean).set" + getCapitalizeName(property.name) + "(" +
                    "beanFromDB.get" + getCapitalizeName(property.name) + "());");
            if (isPropertyAreDictionary(property)) {
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                cf.println("else{\n" +
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);");
                String[] mappedProps = mapping.mappedProperties.stream().map(s -> s.name).toArray(String[]::new);
                String[] mappingProps = rootBuilder.mappings.get(bean.interfaceType + "." + property.name).properties
                        .stream().map(s -> s.name).toArray(String[]::new);
                if (isSingleToMany(bean, property)) {
                    cf.println("//isSingleToMany");
                    for (int i = 0; i < mappedProps.length; i++)
                        cf.println("if(bean.get" + getCapitalizeName(property.name) + "() != null)\n" +
                                "bean.get" + getCapitalizeName(property.name) + "().set" + getCapitalizeName(mappedProps[i]) +
                                "( bean.get" + getCapitalizeName(mappingProps[i]) + "());");
                    cf.println("dao.save(bean.get" + getCapitalizeName(property.name) + "(), updateOptions, dData, savedBeans);\n");
                }
                if (isManyToSingle(bean, property)) {
                    cf.println("dao.save(bean.get" + getCapitalizeName(property.name) + "(), updateOptions, dData, savedBeans);\n");
                    for (int i = 0; i < mappedProps.length; i++)
                        cf.println("if(bean.get" + getCapitalizeName(property.name) + "() != null)\n" +
                                "((" + beanImpl + ")bean).set" + getCapitalizeName(mappingProps[i]) +
                                "(bean.get" + getCapitalizeName(property.name) + "().get" + getCapitalizeName(mappedProps[i]) + "());");
                }
                cf.println("}");
                continue;
            }
            if (property.isCollection() && (mapping != null)) {
                String daoClassNameMappedBean = rootBuilder.repositoriesByBean.get(property.mappedType.toString()).daoClassName;
                String inversionalKey = rootBuilder.beansByInterface.get(property.mappedType.toString()).inversionalKey;
                cf.println("else{\n" +
                        daoClassNameMappedBean + " dao = dData.getRepository(" + daoClassNameMappedBean + ".class);\n" +
                        "java.util.List<" + inversionalKey + ">" + " keysForDeleted =  ");
                if (rootBuilder.beansByInterface.get(property.mappedType.toString()).versionalType == null)
                    cf.println("beanFromDB.get" + capitalizeName +
                            "().stream().map(s -> dao.getKey(s)).collect(java.util.stream.Collectors.toList());\n" +
                            "java.util.List<" + inversionalKey + "> keysForSaved = new java.util.ArrayList();\n" +
                            "if(bean.get" + capitalizeName + "() != null)\n" +
                            "   keysForSaved = bean.get" + capitalizeName +
                            "       ().stream().map(s -> dao.getKey(s)).collect(java.util.stream.Collectors.toList());\n");
                else {
                    if (rootBuilder.beansByInterface.get(property.mappedType.toString())
                            .properties.values().stream().filter(s -> s.isId && !s.isVersionFrom).count() == 1) {
                        DataBeanPropertyBuilder dataBeanPropertyBuilder1 =
                                rootBuilder.beansByInterface.get(property.mappedType.toString())
                                        .properties.values().stream().filter(s -> s.isId && !s.isVersionFrom).findAny().get();
                        cf.println("beanFromDB.get" + capitalizeName +
                                "().stream().map(s -> s.get" + getCapitalizeName(dataBeanPropertyBuilder1.name) + "()).collect(java.util.stream.Collectors.toList());\n" +
                                "java.util.List<" + inversionalKey + "> keysForSaved = new java.util.ArrayList();\n" +
                                "if(bean.get" + capitalizeName + "() != null)\n" +
                                "   keysForSaved = bean.get" + capitalizeName +
                                "       ().stream().map(s -> s.get" + getCapitalizeName(dataBeanPropertyBuilder1.name) + "()).collect(java.util.stream.Collectors.toList());\n");
                    } else {
                        cf.println("beanFromDB.get" + capitalizeName +
                                "().stream().map(s -> dao.getKey(s)).collect(java.util.stream.Collectors.toList());\n" +
                                "java.util.List<" + inversionalKey + "> keysForSaved = new java.util.ArrayList();\n" +
                                "if(bean.get" + capitalizeName + "() != null)\n" +
                                "   keysForSaved = bean.get" + capitalizeName +
                                "       ().stream().map(s -> dao.getKey(s)).collect(java.util.stream.Collectors.toList());\n");
                    }
                }
                cf.println(
                        "if(!keysForDeleted.isEmpty()){\n" +
                                "   keysForDeleted.removeAll(keysForSaved);\n" +
                                "   for (" + inversionalKey + " forDelete: keysForDeleted)\n" +
                                "       dao.delete(forDelete);}\n" +
                                "if(!keysForSaved.isEmpty()){\n" +
                                "   for(" + property.mappedType + " b : bean.get" + capitalizeName + "()){\n" +
                                "       dao.save(b, updateOptions,dData,savedBeans);}" +
                                "}\n}");
                continue;
            }
            if (mapping != null && !property.isCollection()) {
                String daoClassName = rootBuilder.repositoriesByBean.get(property.type.toString()).daoClassName;
                String[] mappedProps = mapping.mappedProperties.stream().map(s -> s.name).toArray(String[]::new);
                String[] mappingProps = rootBuilder.mappings.get(bean.interfaceType + "." + property.name).properties
                        .stream().map(s -> s.name).toArray(String[]::new);

                cf.println("else{" +
                        "if(bean.get" + capitalizeName + "() != null ){\n" +
                        rootBuilder.beansByInterface.get(property.mappedType.toString()).inversionalKey +
                        " savedKey = dData.getRepository(" +
                        daoClassName +
                        ".class).save(" + "bean.get" + capitalizeName + "()," +
                        "updateOptions,dData,savedBeans);");

                cf.println("if(savedKey != null){\n" +
                        property.mappedType + " savedInnerBean = dData.getRepository(" +
                        daoClassName +
                        ".class).get(savedKey);");
                for (int i = 0; i < mappedProps.length; i++)
                    cf.println("((" + bean.getImplementationName() + ")bean).set" + getCapitalizeName(mappingProps[i])
                            + "(savedInnerBean.get" + getCapitalizeName(mappedProps[i]) + "());");
                cf.println("}");

                if (mapping.mappedProperties.stream().anyMatch(s -> s.isId()) &&
                        rootBuilder.mappings.get(bean.interfaceType + "." + property.name).properties
                                .stream().anyMatch(s -> s.isId()))
                    cf.println("}else{\n" + daoClassName + " dao = dData.getRepository(" + daoClassName + ".class);\n" +
                            rootBuilder.beansByInterface.get(property.mappedType.toString()).inversionalKey +
                            " k = dao.getKey(beanFromDB.get" + capitalizeName + "());\n" +
                            "if( k != null ) dao.delete(k);\n" +
                            "}\n}");
                else
                    cf.println("}}");
            }
        }
    }

    private boolean isSingleToMany(DataBeanBuilder bean, DataBeanPropertyBuilder property) {
        return rootBuilder.mappings.get(bean.interfaceType + "." + property.name) == null
                && rootBuilder.mappings.get(
                property.type + bean.interfaceType.toString().substring(bean.interfaceType.toString().lastIndexOf('.'))
        ) != null;
    }

    private boolean isManyToSingle(DataBeanBuilder bean, DataBeanPropertyBuilder property) {
        return rootBuilder.mappings.get(bean.interfaceType + "." + property.name) != null
                && rootBuilder.mappings.get(
                property.type + bean.interfaceType.toString().substring(bean.interfaceType.toString().lastIndexOf('.'))
        ) == null;
    }

    private boolean isMapingIdOnId(DataBeanBuilder bean, DataBeanPropertyBuilder property) {
        if (rootBuilder.mappings.get(property.dataBean.interfaceType + "." + property.name) != null &&
                rootBuilder.mappings.get(property.dataBean.interfaceType + "." + property.name).stream().allMatch(s -> s.property.isId)
                && rootBuilder.mappings.get(property.dataBean.interfaceType + "." + property.name).stream().allMatch(s -> s.mappedProperty.isId))
            return true;
        return false;
    }

    private void mapOfAttributesBuilder(JavaClassWriter cf, DataBeanBuilder bean) throws IOException {
        cf.println("private static java.util.Set<org.docero.data.utils.DDataAttribute> attributes;\n" +
                "private static java.util.Set<org.docero.data.utils.DDataAttribute> attributesMarkedJsonIgnore;\n" +
                "private static java.util.Set<org.docero.data.utils.DDataAttribute> attributesMarkedXmlTransient;\n" +
                "   static {\n" +
                "       java.util.Set set = new java.util.HashSet<>();\n" +
                "");
        for (DataBeanPropertyBuilder property : bean.properties.values()) {
            cf.println("       set.add(" + bean.interfaceType + "_WB_." + property.enumName + ");");
        }
        cf.println("       attributes = java.util.Collections.unmodifiableSet(set);");
        mapOfTransientAttributesBuilder(cf, bean);
        cf.println("    }");
    }

    private void mapOfTransientAttributesBuilder(JavaClassWriter cf, DataBeanBuilder bean) throws IOException {
        String annotation = "XmlTransient";
        mapOfMarkedAnnotationAdder(cf, bean, annotation);
        annotation = "JsonIgnore";
        mapOfMarkedAnnotationAdder(cf, bean, annotation);
    }

    private void mapOfMarkedAnnotationAdder(JavaClassWriter cf, DataBeanBuilder bean, String annotation) throws
            IOException {
        cf.println("set = new java.util.HashSet<>();");
        for (DataBeanPropertyBuilder s : bean.properties.values())
            if (s.getterAnnotations.stream().anyMatch(t -> t.toString().endsWith(annotation)) ||
                    s.setterAnnotations.stream().anyMatch(t -> t.toString().endsWith(annotation))
                    )
                cf.println("set.add(" + bean.interfaceType + "_WB_." + s.enumName + ");");
        cf.println("attributesMarked" + annotation + " = java.util.Collections.unmodifiableSet(set);");
    }

    private String getCapitalizeName(String st) {
        return st.substring(0, 1).toUpperCase() + st.substring(1);
    }
}
