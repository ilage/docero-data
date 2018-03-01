package org.docero.data.processor;

import org.docero.data.DDataProperty;
import org.docero.data.DictionaryType;
import org.docero.data.GeneratedValue;
import org.docero.data.GenerationType;
import org.docero.dgen.processor.DGenProperty;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Collectors;

class DataBeanPropertyBuilder {
    final boolean ignored;
    final String name;
    final String enumName;
    final TypeMirror type;
    final int length;
    final boolean nullable;
    final String columnName;
    final boolean isId;
    final boolean isCollection;
    final boolean isMap;
    final boolean isDiscriminator;
    final DataBeanBuilder dataBean;
    final TypeMirror mappedType;
    final boolean isVersionFrom;
    final boolean isVersionTo;
    final GenerationType generatedStrategy;
    final String generatedValue;
    final boolean generatedBefore;
    final String jdbcType;
    final String readerSql;
    final String writerSql;

    static DataBeanPropertyBuilder from(
            DataBeanBuilder bean, DDataProperty ddProperty, ExecutableElement method,
            TypeMirror collectionType, TypeMirror mapType, TypeMirror voidType
    ) {
        String sn = method.getSimpleName().toString();
        String name;
        TypeMirror type;
        if (sn.startsWith("get") || sn.startsWith("has")) {
            name = Character.toLowerCase(sn.charAt(3)) + sn.substring(4);
            type = method.getReturnType();
        } else if (sn.startsWith("set")) {
            name = Character.toLowerCase(sn.charAt(3)) + sn.substring(4);
            type = method.getTypeParameters().size() != 1 ? voidType :
                    method.getTypeParameters().get(0).asType();
        } else if (sn.startsWith("is")) {
            name = Character.toLowerCase(sn.charAt(2)) + sn.substring(3);
            type = method.getReturnType();
        } else {
            name = sn;
            type = method.getReturnType();
        }
        return new DataBeanPropertyBuilder(
                bean, name, type, ddProperty, method, bean.rootBuilder, collectionType, mapType
        );
    }

    static DataBeanPropertyBuilder from(
            DataBeanBuilder bean, DGenProperty dGen, DDataBuilder rootBuilder,
            DDataProperty ddProperty, TypeMirror collectionType,
            TypeMirror mapType) {
        String name = dGen.getName();
        TypeMirror type = dGen.getType();
        return new DataBeanPropertyBuilder(
                bean, name, type, ddProperty, dGen.getElement(), rootBuilder, collectionType, mapType
        );
    }

    private DataBeanPropertyBuilder(
            DataBeanBuilder bean, String name, TypeMirror type,
            DDataProperty ddProperty, Element element,
            DDataBuilder rootBuilder,
            TypeMirror collectionType,
            TypeMirror mapType) {
        this.dataBean = bean;
        this.name = name;
        this.type = type;
        jdbcType = rootBuilder.jdbcTypeFor(this.type);
        GeneratedValue genVal = element.getAnnotation(GeneratedValue.class);
        if (genVal != null) {
            this.generatedStrategy = genVal.strategy();
            if (genVal.value().length() == 0)
                this.generatedValue = genVal.generator();
            else
                this.generatedValue = genVal.value();
            generatedBefore = genVal.before();
        } else {
            this.generatedStrategy = null;
            this.generatedValue = null;
            this.generatedBefore = true;
        }
        this.ignored = ddProperty != null && ddProperty.Transient();
        nullable = ddProperty == null || ddProperty.nullable();
        isVersionFrom = ddProperty != null && ddProperty.versionFrom();
        isVersionTo = ddProperty != null && ddProperty.versionTo();
        length = ddProperty == null ? 0 : ddProperty.length();
        if (ddProperty != null && ddProperty.value().length() > 0) {
            columnName = ddProperty.value();
            readerSql = ddProperty.reader().length() == 0 ? null : ddProperty.reader();
            writerSql = ddProperty.writer().length() == 0 ? null : ddProperty.writer();
            isId = ddProperty.id();
            isDiscriminator = ddProperty.discriminator();
        } else {
            columnName = DataBeanBuilder.propertyName2sqlName(name);
            isId = ddProperty != null && ddProperty.id();
            isDiscriminator = ddProperty != null && ddProperty.discriminator();
            readerSql = null;
            writerSql = null;
        }
        Types typeUtils = rootBuilder.environment.getTypeUtils();
        TypeMirror ltypeErasure = typeUtils.erasure(this.type);
        isCollection = typeUtils.isSubtype(ltypeErasure, collectionType);
        isMap = typeUtils.isSubtype(ltypeErasure, mapType);
        if (isCollection) mappedType =
                typeUtils.erasure(((DeclaredType) this.type).getTypeArguments().get(0));
        else if (isMap) mappedType =
                typeUtils.erasure(((DeclaredType) this.type).getTypeArguments().get(1));
        else mappedType = this.type.getKind().isPrimitive() ?
                    typeUtils.boxedClass((PrimitiveType) this.type).asType() :
                    typeUtils.erasure(this.type);

        StringBuilder elemName = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (Character.isUpperCase(c)) elemName.append('_').append(c);
            else elemName.append(Character.toUpperCase(c));
        }
        enumName = elemName.toString();
    }

    void buildProperty(JavaClassWriter cf) throws IOException {
        DataBeanBuilder mappedBean = this.dataBean.rootBuilder.beansByInterface.get(mappedType.toString());
        if (mappedBean != null) {
            cf.println("@javax.xml.bind.annotation.XmlElement(type = " +
                    mappedBean.getImplementationName() + ".class)");
        } else printKnownXmlAdapters(cf, type);

        cf.println("private " + type.toString() + " " + name + ";");
    }

    static void printKnownXmlAdapters(JavaClassWriter cf, TypeMirror type) throws IOException {
        if ("java.time.LocalDate".equals(type.toString()))
            cf.println("@javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter(type = java.time.LocalDate.class, " +
                    "value = org.docero.data.utils.LocalDateAdapter.class)");
        else if ("java.time.LocalTime".equals(type.toString()))
            cf.println("@javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter(type = java.time.LocalTime.class, " +
                    "value = org.docero.data.utils.LocalTimeAdapter.class)");
        else if ("java.time.LocalDateTime".equals(type.toString()))
            cf.println("@javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter(type = java.time.LocalDateTime.class, " +
                    "value = org.docero.data.utils.LocalDateTimeAdapter.class)");
        else if ("java.time.OffsetDateTime".equals(type.toString()))
            cf.println("@javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter(type = java.time.OffsetDateTime.class, " +
                    "value = org.docero.data.utils.OffsetDateTimeAdapter.class)");
    }

    void buildGetter(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public " +
                type.toString() + " get" +
                Character.toUpperCase(name.charAt(0)) +
                name.substring(1) + "() {"
        );
        DataBeanBuilder mappedBean = this.dataBean.rootBuilder.beansByInterface.get(mappedType.toString());
        if (mappedBean != null && !isCollectionOrMap() && mappedBean.dictionary != DictionaryType.NO) {
            Mapping mapping = this.dataBean.rootBuilder.mappings.get(this.dataBean.interfaceType + "." + this.name);
            if (mapping != null) {
                DataBeanPropertyBuilder mProp = mapping.properties.get(0);
                String getter = "this.get" +
                        Character.toUpperCase(mProp.name.charAt(0)) + mProp.name.substring(1) + "()";
                if (mProp.type.getKind().isPrimitive())
                    cf.startBlock("if(" + name + " == null && " + getter + " != 0) {");
                else
                    cf.startBlock("if(" + name + " == null && " + getter + " != null) {");
                cf.println(name + " = cached(" + mappedType + ".class," + getter + ");");
                cf.endBlock("}");
            }
        }
        cf.println("return " + name + ";");
        cf.endBlock("}");
    }

    void buildSetter(JavaClassWriter cf) throws IOException {
        cf.println("");
        cf.startBlock("public void set" +
                Character.toUpperCase(name.charAt(0)) +
                name.substring(1) + "(" +
                type.toString() + " " +
                name + ") {"
        );
        cf.println("this." + name + " = " + name + ";");
        if (notCollectionOrMap()) {
            Mapping mapping = this.dataBean.rootBuilder.mappings.get(this.dataBean.interfaceType + "." + this.name);
            if (mapping != null) mapping.stream()
                    .forEach(m -> {
                        try {
                            String setter = "this.set" +
                                    Character.toUpperCase(m.property.name.charAt(0)) + m.property.name.substring(1);
                            String getter = name + ".get" +
                                    Character.toUpperCase(m.mappedProperty.name.charAt(0)) +
                                    m.mappedProperty.name.substring(1);
                            cf.println((m.property.isVersionFrom ? "//" : "") +
                                    setter + "(" + name + " == null ? " +
                                    (m.property.type.getKind().isPrimitive() ? "0" : "null") +
                                    " : " + getter + "());");
                        } catch (IOException ignore) {
                        }
                    });
        }
        cf.endBlock("}");
    }

    void buildEnumElement(JavaClassWriter cf, HashMap<String, DataBeanBuilder> beansByInterface, ProcessingEnvironment environment) throws IOException {
        TypeMirror typeErasure = environment.getTypeUtils().erasure(isCollection ?
                ((DeclaredType) type).getTypeArguments().get(0) : type);
        DataBeanBuilder manType = beansByInterface.get(typeErasure.toString());
        if (manType == null) {
            cf.println("/** Value of column " + this.columnName + "*/");
            cf.println(this.enumName + "(\"" +
                    this.columnName + "\",\"" +
                    this.name + "\"," +
                    (this.type.getKind().isPrimitive() ?
                            environment.getTypeUtils().boxedClass((PrimitiveType) this.type).asType() :
                            environment.getTypeUtils().erasure(this.type)
                    ) + ".class,\"" + this.jdbcType + "\", false, false, " +
                    this.isCollection + "," + this.isId + "),");

            if (this.isVersionFrom)
                cf.println("VERSION_(\"" +
                        this.columnName + "\",\"" +
                        this.name + "\"," +
                        (this.type.getKind().isPrimitive() ?
                                environment.getTypeUtils().boxedClass((PrimitiveType) this.type).asType() :
                                environment.getTypeUtils().erasure(this.type)
                        ) + ".class,\"" + this.jdbcType + "\", false, false, " +
                        this.isCollection + "," + this.isId + "),");
        }
    }

    void buildEnumElementWithBeans(
            JavaClassWriter cf,
            HashMap<String, DataBeanBuilder> beansByInterface,
            HashMap<String, Mapping> mappings,
            ProcessingEnvironment environment
    ) throws IOException {
        TypeMirror typeErasure = environment.getTypeUtils().erasure(isCollection ?
                ((DeclaredType) type).getTypeArguments().get(0) : type);
        DataBeanBuilder manType = beansByInterface.get(typeErasure.toString());
        if (manType == null) {
            cf.println("/** Value of column " + this.columnName + "*/");
            cf.println(this.enumName + "(\"" +
                    this.columnName + "\",\"" +
                    this.name + "\"," +
                    (this.type.getKind().isPrimitive() ?
                            environment.getTypeUtils().boxedClass((PrimitiveType) this.type).asType() :
                            environment.getTypeUtils().erasure(this.type)
                    ) + ".class,\"" + this.jdbcType + "\", false, false, " + this.isCollection +
                    ", null, null," + this.isId + "),");
        } else {
            cf.println("/** Value of column " + this.columnName + "*/");
            cf.println(this.enumName + "(\"" +
                    this.columnName + "\",\"" +
                    this.name + "\"," +
                    manType.interfaceType + "_WB_.class,\"" + (this.isSimple() ? "" : "ARRAY") +
                    "\"," + manType.isDictionary() + ", true" + ", " + this.isCollection +
                    ",\"" + manType.getTableRef().replace("\"", "\\\"") + "\", new java.util.HashMap<String, String>(){{" +
                    mappings.get(dataBean.interfaceType.toString() + "." + this.name).stream()
                            .map(m -> "put(\"" + m.property.columnName + "\",\"" + m.mappedProperty.columnName + "\");")
                            .collect(Collectors.joining(" ")) +
                    "}}" + ", false),");
        }
    }

    boolean isId() {
        return isId;
    }

    boolean isCollection() {
        return isCollection;
    }

    boolean isCollectionOrMap() {
        return isCollection || isMap;
    }

    boolean notCollectionOrMap() {
        return !(isCollection || isMap);
    }

    boolean isSimple() {
        return !(isCollection || isMap);
    }

    String getColumnRef() {
        return columnName == null ? null : "\"" + columnName + "\"";
    }

    String getColumnReader(int tableIndex) {
        return readerSql == null ?
                "t" + tableIndex + ".\"" + columnName + "\"" :
                readerSql.replace("?", "t" + tableIndex + ".\"" + columnName + "\"");
    }

    String getColumnWriter(String parameterName) {
        return writerSql == null ? parameterName : writerSql.replace("?", parameterName);
    }

    boolean notId() {
        return !isId;
    }

    boolean isGenerated() {
        return generatedStrategy != null;
    }

    boolean notGenerated() {
        return generatedStrategy == null;
    }

    boolean notIgnored() {
        return !ignored;
    }
}
