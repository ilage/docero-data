package org.docero.data.processor;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.DDataVersionalRepository;
import org.docero.data.DictionaryType;
import org.docero.data.utils.DDataDictionary;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import java.io.IOException;
import java.util.*;
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
            if (bean == null) {
                // if bean was not created by any extending beans (not single interface in declaration)
                bean = new DataBeanBuilder(
                        rootBuilder.environment.getElementUtils().getTypeElement(forInterfaceName.toString()),
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

    void build(ProcessingEnvironment environment, boolean spring) throws IOException {
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
            cf.startBlock("public final class " +
                    daoClassName.substring(simpNameDel + 1) +
                    " extends org.docero.data.AbstractRepository<" + bean.interfaceType + "," + bean.inversionalKey + ">" +
                    " implements " + repositoryInterface + (bean.isDictionary() ?
                    ", org.docero.data.utils.DDataDictionary<" +
                            bean.interfaceType + "," + bean.inversionalKey + ">" :
                    "") + " {"
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
                if (spring) cf.println("@org.springframework.cache.annotation.CachePut(cacheNames=\"" +
                        bean.cacheMap + "\", key = \"#bean.dDataBeanKey_\")");
                cf.startBlock("public <T extends " + forInterfaceName + "> T put_(T bean) {");
                //TODO without Spring
                cf.println("return bean;");
                cf.endBlock("}");
                cf.println("");
                cf.startBlock("public void putList_(java.util.List<" + bean.keyType + "> bean) {");
                //TODO without Spring
                cf.endBlock("}");
            }

            buildMethodCreate(cf);
            if (defaultGetMethod == null) {
                new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.GET).build(cf);
                //buildMethodGet(cf);
            }
            if (!hasInsert) {
                new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.INSERT).build(cf);
                //buildMethodInsert(cf, discriminator);
            }
            if (!hasUpdate) {
                new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.UPDATE).build(cf);
                //buildMethodUpdate(cf, discriminator);
            }
            if (defaultDeleteMethod == null) {
                new DDataMethodBuilder(this, bean, DDataMethodBuilder.MType.DELETE).build(cf);
                //buildMethodDelete(cf);
            }
            if (bean.dictionary != DictionaryType.NO && defaultListMethod == null) {
                cf.println("");
                cf.startBlock("public java.util.List<" + forInterfaceName + "> list() {");
                cf.println("return getSqlSession().selectList(\"" +
                        mappingClassName + ".dictionary\");");
                cf.endBlock("}");
            }

            for (DDataMethodBuilder method : methods) method.build(cf);

            cf.endBlock("}");
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
            cf.println("SQL query, can contains method parameter names (like <i>:parameterName</i>)");
            cf.println("<p>Can call stored procedure that returns table of DDataRepository data beans, or specified resultMap</p>");
            cf.println("@return SQL query");
            cf.endBlock("*/");
            cf.println("String select() default \"\";");

            cf.startBlock("/**");
            cf.println("Used with 'select' parameter");
            cf.println("@return Custom resultMap name used for mapping results");
            cf.endBlock("*/");
            cf.println("String resultMap() default \"\";");

            cf.startBlock("/**");
            cf.println("Fields what being not loaded at all, nor EAGER, nor LAZY");
            cf.println("@return ignored fields");
            cf.endBlock("*/");
            cf.println(forInterfaceName + "_[] ignore() default " + forInterfaceName + "_.NONE_;");

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
}
