package org.docero.data.processor;

import org.docero.data.DDataProperty;
import org.docero.data.DictionaryType;
import org.docero.data.GeneratedValue;
import org.docero.data.GenerationType;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.HashMap;

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

    DataBeanPropertyBuilder(
            DataBeanBuilder bean, DDataProperty ddProperty, ExecutableElement method,
            ProcessingEnvironment environment,
            TypeMirror collectionType, TypeMirror mapType, TypeMirror voidType
    ) {
        this.dataBean = bean;
        this.ignored = ddProperty != null && ddProperty.Trancient();
        GeneratedValue genVal = method.getAnnotation(GeneratedValue.class);
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
        String sn = method.getSimpleName().toString();
        if (sn.startsWith("get") || sn.startsWith("has")) {
            name = Character.toLowerCase(sn.charAt(3)) + sn.substring(4);
            this.type = method.getReturnType();
        } else if (sn.startsWith("set")) {
            name = Character.toLowerCase(sn.charAt(3)) + sn.substring(4);
            this.type = method.getTypeParameters().size() != 1 ? voidType :
                    method.getTypeParameters().get(0).asType();
        } else if (sn.startsWith("is")) {
            name = Character.toLowerCase(sn.charAt(2)) + sn.substring(3);
            this.type = method.getReturnType();
        } else {
            name = sn;
            this.type = method.getReturnType();
        }
        jdbcType = dataBean.rootBuilder.jdbcTypeFor(this.type);
        nullable = ddProperty == null || ddProperty.nullable();
        isVersionFrom = ddProperty != null && ddProperty.versionFrom();
        isVersionTo = ddProperty != null && ddProperty.versionTo();
        length = ddProperty == null ? 0 : ddProperty.length();
        if (ddProperty != null && ddProperty.value().length() > 0) {
            columnName = ddProperty.value();
            readerSql = ddProperty.reader().length() == 0 ? null : ddProperty.reader();
            writerSql = ddProperty.writer().length() == 0 ? null : ddProperty.writer();
            isId = ddProperty.id();
        } else {
            StringBuilder nameBuilder = new StringBuilder();
            for (char c : name.toCharArray())
                if (nameBuilder.length() == 0)
                    nameBuilder.append(Character.toLowerCase(c));
                else if (Character.isUpperCase(c))
                    nameBuilder.append('_').append(Character.toLowerCase(c));
                else nameBuilder.append(c);
            columnName = nameBuilder.toString();
            isId = ddProperty != null && ddProperty.id();
            readerSql = null;
            writerSql = null;
        }
        TypeMirror ltypeErasure = environment.getTypeUtils().erasure(this.type);
        isCollection = environment.getTypeUtils().isSubtype(ltypeErasure, collectionType);
        isMap = environment.getTypeUtils().isSubtype(ltypeErasure, mapType);
        if (isCollection) mappedType =
                environment.getTypeUtils().erasure(((DeclaredType) this.type).getTypeArguments().get(0));
        else if (isMap) mappedType =
                environment.getTypeUtils().erasure(((DeclaredType) this.type).getTypeArguments().get(1));
        else mappedType = this.type.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) this.type).asType() :
                    environment.getTypeUtils().erasure(this.type);

        StringBuilder elemName = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (Character.isUpperCase(c)) elemName.append('_').append(c);
            else elemName.append(Character.toUpperCase(c));
        }
        enumName = elemName.toString();
    }

    void buildProperty(JavaClassWriter cf) throws IOException {
        cf.println("private " + type.toString() + " " + name + ";");
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
                    cf.startBlock("if(dictionariesService!=null && " + name + " == null && " + getter + " != 0) {");
                else
                    cf.startBlock("if(dictionariesService!=null && " + name + " == null && " + getter + " != null) {");
                cf.println(name + " = dictionariesService.get(" + mappedType + ".class," + getter + ");");
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
            if (mapping != null) {
                mapping.stream().forEach(m -> {
                    try {
                        DataBeanPropertyBuilder mProp = m.properties.get(0);
                        String setter = "this.set" +
                                Character.toUpperCase(mProp.name.charAt(0)) + mProp.name.substring(1);
                        String getter = name + ".get" +
                                Character.toUpperCase(m.mappedProperties.get(0).name.charAt(0)) + m.mappedProperties.get(0).name.substring(1);
                        cf.println(setter + "(" + name + " == null ? " +
                                (mProp.type.getKind().isPrimitive() ? "0" : "null") +
                                " : " + getter + "());");
                    } catch (IOException ignore) {
                    }
                });
            }
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
                    ) + ".class,\"" + this.jdbcType + "\"),");
        }
    }

    void buildEnumElementWithBeans(JavaClassWriter cf, HashMap<String, DataBeanBuilder> beansByInterface, ProcessingEnvironment environment) throws IOException {
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
                    ) + ".class,\"" + this.jdbcType + "\"),");
        } else {
            cf.println("/** Value of column " + this.columnName + "*/");
            cf.println(this.enumName + "(\"" +
                    this.columnName + "\",\"" +
                    this.name + "\"," +
                    manType.interfaceType + "_WB_.class,\"" + (this.isSimple() ? "" : "ARRAY") + "\"),");
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
