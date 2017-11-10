package org.docero.data.processor;

import org.docero.data.DDataRep;
import org.docero.data.DictionaryType;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.*;

class DataRepositoryBuilder {
    final String name;
    final TypeMirror forInterfaceName;
    final TypeMirror idClass;
    final TypeMirror actualTimeClass;
    final String daoClassName;
    final String restClassName;
    final TypeMirror repositoryInterface;
    final String beanImplementation;
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

    DataRepositoryBuilder(
            DDataBuilder rootBuilder,
            TypeElement repositoryElement
    ) {
        DDataRep ddRep = repositoryElement.getAnnotation(DDataRep.class);
        repositoryInterface = repositoryElement.asType();
        this.mappingClassName = rootBuilder.environment.getTypeUtils().erasure(repositoryInterface).toString();
        if (ddRep.value().length() > 0) name = ddRep.value();
        else name = mappingClassName.substring(mappingClassName.lastIndexOf('.'));
        this.rootBuilder = rootBuilder;

        boolean isHistorical = false;
        DeclaredType interfaceType = null;
        for (TypeMirror i : repositoryElement.getInterfaces()) {
            isHistorical = i.toString().contains("org.docero.data.DDataVersionalRepository");
            if (isHistorical || i.toString().contains("org.docero.data.DDataRepository"))
                interfaceType = (DeclaredType) i;
            if (isHistorical) break;
        }

        String str = repositoryElement.getSimpleName().toString();
        repositoryVariableName = Character.toLowerCase(str.charAt(0)) + str.substring(1);

        if (interfaceType != null) {
            List<? extends TypeMirror> gens = interfaceType.getTypeArguments();
            forInterfaceName = gens.get(0);
            idClass = gens.get(1);
            actualTimeClass = !isHistorical ? null : gens.get(2);
            beanImplementation = rootBuilder.beansByInterface.get(forInterfaceName.toString())
                    .getImplementationName();
            this.versionalType = !isHistorical ? null : gens.get(2);
        } else {
            forInterfaceName = null;
            idClass = null;
            actualTimeClass = null;
            beanImplementation = null;
            this.versionalType = null;
        }
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
                "get".equals(m.methodName) &&
                        m.parameters.size() == (versionalType == null ? 1 : 2) &&
                        rootBuilder.environment.getTypeUtils().isSameType(m.parameters.get(0).type, idClass))
                .findAny().orElse(null);
        defaultDeleteMethod = methods.stream().filter(m ->
                "delete".equals(m.methodName) && m.parameters.size() == 1 &&
                        rootBuilder.environment.getTypeUtils().isSameType(m.parameters.get(0).type, idClass))
                .findAny().orElse(null);
        defaultListMethod = methods.stream().filter(m ->
                "list".equals(m.methodName) && m.parameters.size() == 0)
                .findAny().orElse(null);
        hasInsert = methods.stream().anyMatch(m -> "insert".equals(m.methodName));
        hasUpdate = methods.stream().anyMatch(m -> "update".equals(m.methodName));
    }

    private DataRepositoryBuilder(DDataBuilder rootBuilder, DataBeanBuilder bean) {
        this.forInterfaceName = bean.interfaceType;
        this.idClass = rootBuilder.environment.getElementUtils().getTypeElement(bean.inversionalKey).asType();
        this.actualTimeClass = bean.versionalType;
        this.daoClassName = bean.interfaceType + "_Dao_";
        this.restClassName = null;
        this.mappingClassName = rootBuilder.environment.getTypeUtils().erasure(forInterfaceName).toString();
        String str = mappingClassName.substring(mappingClassName.lastIndexOf('.') + 1) + "Repository";
        repositoryVariableName = Character.toLowerCase(str.charAt(0)) + str.substring(1);
        this.versionalType = bean.versionalType;
        this.repositoryInterface = versionalType != null ?
                rootBuilder.environment.getTypeUtils().getDeclaredType(
                        rootBuilder.environment.getElementUtils().getTypeElement("org.docero.data.DDataVersionalRepository"),
                        forInterfaceName, idClass, versionalType) :
                rootBuilder.environment.getTypeUtils().getDeclaredType(
                        rootBuilder.environment.getElementUtils().getTypeElement("org.docero.data.DDataRepository"),
                        forInterfaceName, idClass);
        this.name = mappingClassName.substring(mappingClassName.lastIndexOf('.'));
        this.beanImplementation = bean.getImplementationName();
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

    void build(ProcessingEnvironment environment, boolean spring) throws IOException {
        int simpNameDel = daoClassName.lastIndexOf('.');

        buildFilterAnnotation(environment);
        buildDataFetchAnnotation(environment);
        // buildAnnotationsAndEnums bean implementation
        try (JavaClassWriter cf = new JavaClassWriter(environment, daoClassName)) {
            cf.println("package " +
                    daoClassName.substring(0, simpNameDel) + ";");

            String beanPkg = forInterfaceName.toString();
            beanPkg = beanPkg.substring(0, beanPkg.lastIndexOf('.'));
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
            if (defaultGetMethod == null) buildMethodGet(cf);
            if (!hasInsert) buildMethodInsert(cf);
            if (!hasUpdate) buildMethodUpdate(cf);
            if (defaultDeleteMethod == null) buildMethodDelete(cf);

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
            cf.startBlock("public @interface " + annotName + " {");
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
            cf.println("@Target(ElementType.METHOD)");
            cf.startBlock("public @interface " + annotName + " {");

            cf.startBlock("/**");
            cf.println("Load mapped entities in single select<br>");
            cf.println("DDataFetchType.NO - truncate any association and collection attributes from loads " +
                    "(like a eagerTrunkLevel=0 with truncateMapped=true)<br>");
            cf.println("DDataFetchType.LAZY - do lazy loads <br>");
            cf.println("DDataFetchType.EAGER - do eager loads up to eagerTrunkLevel<br>");
            cf.println("DDataFetchType.COLLECTIONS_ARE_LAZY - do eager load for associations and lazy for collections<br>");
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
            cf.println("Level for EAGER loaded beans associations and collections attributes<br>");
            cf.println("0 - load all lazy<br>");
            cf.println("1 - provide eager load of associations and collections attributes in first level of eager loaded beans, " +
                    "and lazy for 2-nd level<br>");
            cf.println("default 1");
            cf.println("@return eager loads truncation");
            cf.endBlock("*/");
            cf.println("int eagerTrunkLevel() default 1;");

            cf.startBlock("/**");
            cf.println("Truncate LAZY loaded beans associations and collections attributes<br>");
            cf.println("default false");
            cf.println("@return do truncation for mapped beans from eagerTrunkLevel");
            cf.endBlock("*/");
            cf.println("boolean truncateLazy() default false;");

            cf.endBlock("}");
        }
    }

    static DataRepositoryBuilder build(
            DDataBuilder rootBuilder,
            DataBeanBuilder bean
    ) throws IOException {
        DataRepositoryBuilder builder = new DataRepositoryBuilder(rootBuilder, bean);
        builder.build(rootBuilder.environment, rootBuilder.spring);
        return builder;
    }

    private void buildMethodDelete(JavaClassWriter cf) throws IOException {
        DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());
        cf.println("");
        if (versionalType != null) {
            cf.startBlock("public void delete(" +
                    idClass + " id) {");
            cf.println("getSqlSession().delete(\"" +
                    mappingClassName + ".delete\", new " + bean.keyType + "(id));");
            cf.endBlock("}");
        } else {
            if (bean.dictionary != DictionaryType.NO && rootBuilder.useSpringCache)
                cf.println("@org.springframework.cache.annotation.CacheEvict(cacheNames=\"" + bean.cacheMap + "\")");

            cf.startBlock("public void delete(" +
                    idClass + " id) {");
            cf.println("getSqlSession().delete(\"" +
                    mappingClassName + ".delete\", id);");
            cf.endBlock("}");
        }
    }

    private void buildMethodUpdate(JavaClassWriter cf) throws IOException {
        DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());
        cf.println("");
        if (versionalType != null) {
            cf.startBlock("public void update(" +
                    forInterfaceName + " bean) {");
            cf.println(bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny().map(p ->
                    "bean.set" + Character.toUpperCase(p.name.charAt(0)) + p.name.substring(1) +
                            "(" + DataBeanBuilder.dateNowFrom(bean.versionalType) + ");")
                    .orElse("")
            );
            cf.println("getSqlSession().insert(\"" +
                    mappingClassName + ".insert\", bean);");
            cf.endBlock("}");
        } else {
            cf.startBlock("public void update(" +
                    forInterfaceName + " bean) {");
            cf.println("getSqlSession().update(\"" +
                    mappingClassName + ".update\", bean);");
            if (bean.dictionary != DictionaryType.NO)
                cf.println("cache(bean);");
            cf.endBlock("}");
        }
    }

    private void buildMethodInsert(JavaClassWriter cf) throws IOException {
        DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());

        cf.println("");
        cf.startBlock("public void insert(" +
                forInterfaceName + " bean) {");
        if (versionalType != null) {
            cf.println(bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny().map(p ->
                    "bean.set" + Character.toUpperCase(p.name.charAt(0)) + p.name.substring(1) +
                            "(" + DataBeanBuilder.dateNowFrom(bean.versionalType) + ");")
                    .orElse("")
            );
        }
        cf.println("getSqlSession().insert(\"" +
                mappingClassName + ".insert\", bean);");
        if (bean.dictionary != DictionaryType.NO)
            cf.println("cache(bean);");
        cf.endBlock("}");
    }

    private void buildMethodGet(JavaClassWriter cf) throws IOException {
        DataBeanBuilder bean = rootBuilder.beansByInterface.get(forInterfaceName.toString());
        cf.println("");
        if (versionalType == null) {
            if (bean.dictionary != DictionaryType.NO && rootBuilder.useSpringCache)
                cf.println("@org.springframework.cache.annotation.Cacheable(cacheNames=\"" + bean.cacheMap + "\", sync=true)");

            cf.startBlock("public " + forInterfaceName + " get(" +
                    idClass + " id) {");
            if (bean.dictionary == DictionaryType.SMALL) {
                cf.println("java.util.List<" + beanImplementation + "> l = getSqlSession().selectList(\"" +
                        mappingClassName + ".dictionary\");");
                cf.println("cache(l);");
                cf.println("return l.stream().filter(r->java.util.Objects.equals(id, r.getDDataBeanKey_())).findAny().orElse(null);");
            } else cf.println("return getSqlSession().selectOne(\"" +
                    mappingClassName + ".get\", id);");
            cf.endBlock("}");
        } else {
            cf.startBlock("public " + forInterfaceName + " get(" +
                    idClass + " id) {");
            cf.println("return getSqlSession().selectOne(\"" +
                    mappingClassName + ".get\", new " + bean.keyType + "(id));");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public " + forInterfaceName + " get(" +
                    idClass + " id, " +
                    actualTimeClass + " at) {");
            cf.println("return getSqlSession().selectOne(\"" +
                    mappingClassName + ".get\", new " + bean.keyType + "(id, at));");
            cf.endBlock("}");
        }
    }

    private void buildMethodCreate(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public " + forInterfaceName + " create() {");
        cf.println("return new " + beanImplementation + "();");
        cf.endBlock("}");
    }
}
