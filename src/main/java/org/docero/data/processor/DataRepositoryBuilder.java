package org.docero.data.processor;

import org.docero.data.DDataRep;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
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
    final boolean isCreatedByInterface;
    final boolean isHistorical;
    final ArrayList<DDataMethodBuilder> methods = new ArrayList<>();
    final DDataBuilder rootBuilder;
    final String mappingClassName;

    final DDataMethodBuilder defaultGetMethod;
    final DDataMethodBuilder defaultDeleteMethod;
    final boolean hasInsert;
    final boolean hasUpdate;
    final HashMap<String, org.w3c.dom.Element> lazyLoads = new HashMap<>();

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
            isHistorical = i.toString().contains("org.docero.data.DDataHistoryRepository");
            if (isHistorical || i.toString().contains("org.docero.data.DDataRepository"))
                interfaceType = (DeclaredType) i;
            if (isHistorical) break;
        }
        this.isHistorical = isHistorical;

        isCreatedByInterface = repositoryElement.getKind() == ElementKind.INTERFACE;

        if (interfaceType != null) {
            List<? extends TypeMirror> gens = interfaceType.getTypeArguments();
            forInterfaceName = gens.get(0);
            idClass = gens.get(1);
            actualTimeClass = !isHistorical ? null : gens.get(2);
            beanImplementation = rootBuilder.beansByInterface.get(forInterfaceName.toString())
                    .getImplementationName();
        } else {
            forInterfaceName = null;
            idClass = null;
            actualTimeClass = null;
            beanImplementation = null;
        }
        daoClassName = repositoryElement.asType().toString() + "_Dao_";
        restClassName = repositoryElement.asType().toString() + "Controller";

        for (Element element : repositoryElement.getEnclosedElements())
            if (element.getKind() == ElementKind.METHOD) {
                if (element.getModifiers().contains(Modifier.ABSTRACT)) {
                    methods.add(new DDataMethodBuilder(this, (ExecutableElement) element));
                }
            }
        defaultGetMethod = methods.stream().filter(m ->
                "get".equals(m.methodName) && m.parameters.size() == 1 && m.parameters.get(0).type == idClass)
                .findAny().orElse(null);
        if (defaultGetMethod != null) defaultGetMethod.setDefault();
        defaultDeleteMethod = methods.stream().filter(m ->
                "delete".equals(m.methodName) && m.parameters.size() == 1 && m.parameters.get(0).type == idClass)
                .findAny().orElse(null);
        hasInsert = methods.stream().anyMatch(m -> "insert".equals(m.methodName));
        hasUpdate = methods.stream().anyMatch(m -> "update".equals(m.methodName));
    }

    private DataRepositoryBuilder(DDataBuilder rootBuilder, DataBeanBuilder bean) {
        this.forInterfaceName = bean.interfaceType;
        Optional<DataBeanPropertyBuilder> idOpt = bean.properties.values().stream().filter(p -> p.isId).findAny();
        this.idClass = idOpt.isPresent() ? (!idOpt.get().type.getKind().isPrimitive() ? idOpt.get().type :
                rootBuilder.environment.getTypeUtils().boxedClass((PrimitiveType) idOpt.get().type).asType()) :
                rootBuilder.environment.getElementUtils().getTypeElement("java.lang.Void").asType();
        this.actualTimeClass = null;
        this.daoClassName = bean.interfaceType + "_Dao_";
        this.restClassName = null;
        this.repositoryInterface = rootBuilder.environment.getTypeUtils().getDeclaredType(
                rootBuilder.environment.getElementUtils().getTypeElement("org.docero.data.DDataRepository"),
                forInterfaceName, idClass);
        this.mappingClassName = rootBuilder.environment.getTypeUtils().erasure(forInterfaceName).toString();
        this.name = mappingClassName.substring(mappingClassName.lastIndexOf('.'));
        this.beanImplementation = bean.getImplementationName();
        this.isCreatedByInterface = true;
        this.isHistorical = false;
        //final ArrayList<DDataMethodBuilder> methods = new ArrayList<>();
        this.rootBuilder = rootBuilder;
        defaultGetMethod = null;
        defaultDeleteMethod = null;
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
        // build bean implementation
        try (JavaClassWriter cf = new JavaClassWriter(environment, daoClassName)) {
            cf.println("package " +
                    daoClassName.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            if (isCreatedByInterface)
                cf.startBlock("public final class " +
                        daoClassName.substring(simpNameDel + 1)
                        + (spring ? " extends org.mybatis.spring.support.SqlSessionDaoSupport" : "") +
                        " implements " + repositoryInterface + " {");
            else
                cf.startBlock("public final class " +
                        daoClassName.substring(simpNameDel + 1)
                        + " extends " + repositoryInterface + " {");

            if (!(spring && isCreatedByInterface)) {
                //TODO without Spring
                cf.startBlock("private org.apache.ibatis.session.SqlSession getSqlSession() {");
                cf.println("return null;");
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
            cf.println("Load mapped entities in single select, default DDataFetchType.COLLECTIONS_ARE_LAZY");
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
            cf.println("Truncate EAGER loaded beans associations and collections attributes<br>");
            cf.println("0 - deny all associations and collections attributes in eager loaded beans<br>");
            cf.println("1 - provide eager load of associations and collections attributes in first level of eager loaded beans, " +
                    "and deny all for 2-nd level<br>");
            cf.println("default -1 - lazy load of associations and collections attributes in eager loaded beans");
            cf.println("@return eager loads truncation");
            cf.endBlock("*/");
            cf.println("int eagerTrunkLevel() default -1;");

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
        cf.println("");
        cf.startBlock("public void delete(" +
                idClass + " id) {");
        cf.println("getSqlSession().delete(\"" +
                mappingClassName + ".delete\", id);");
        cf.endBlock("}");
    }

    private void buildMethodUpdate(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public void update(" +
                forInterfaceName + " bean) {");
        cf.println("getSqlSession().update(\"" +
                mappingClassName + ".update\", bean);");
        cf.endBlock("}");
    }

    private void buildMethodInsert(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public void insert(" +
                forInterfaceName + " bean) {");
        cf.println("getSqlSession().insert(\"" +
                mappingClassName + ".insert\", bean);");
        cf.endBlock("}");
    }

    private void buildMethodGet(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public " + forInterfaceName + " get(" +
                idClass + " id) {");
        cf.println("return getSqlSession().selectOne(\"" +
                mappingClassName + ".get\", id);");
        cf.endBlock("}");

        if (isHistorical) {
            cf.println("");
            cf.startBlock("public " + forInterfaceName + " get(" +
                    idClass + " id, " +
                    actualTimeClass + " at) {");
            cf.println("return null;");
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
