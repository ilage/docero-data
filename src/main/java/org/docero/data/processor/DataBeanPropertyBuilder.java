package org.docero.data.processor;

import org.docero.data.DDataProperty;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class DataBeanPropertyBuilder {
    final String name;
    final String enumName;
    final TypeMirror type;
    final int length;
    final boolean nullable;
    final String columnName;
    final boolean isId;
    final boolean collection;
    final DataBeanBuilder dataBean;
    final String typeErasure;

    DataBeanPropertyBuilder(
            DataBeanBuilder bean, ExecutableElement method, ProcessingEnvironment environment, TypeMirror collectionType
    ) {
        this.dataBean = bean;
        DDataProperty ddProperty = method.getAnnotation(DDataProperty.class);
        String sn = method.getSimpleName().toString();
        if (sn.startsWith("get")) {
            name = Character.toLowerCase(sn.charAt(3)) + sn.substring(4);
            type = method.getReturnType();
        } else if (sn.startsWith("set")) {
            name = Character.toLowerCase(sn.charAt(3)) + sn.substring(4);
            type = method.getTypeParameters().get(0).asType();
        } else if (sn.startsWith("is")) {
            name = Character.toLowerCase(sn.charAt(2)) + sn.substring(3);
            type = method.getReturnType();
        } else {
            name = sn;
            type = method.getReturnType();
        }
        typeErasure = environment.getTypeUtils().erasure(type).toString();
        nullable = ddProperty == null || ddProperty.nullable();
        length = ddProperty == null ? 0 : ddProperty.length();
        if (ddProperty != null && ddProperty.value().length() > 0) {
            columnName = ddProperty.value();
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
            isId = false;
        }
        TypeMirror typeErasure = environment.getTypeUtils().erasure(type);
        collection = environment.getTypeUtils().isSubtype(typeErasure, collectionType);

        StringBuilder elemName = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (Character.isUpperCase(c)) elemName.append('_').append(c);
            else elemName.append(Character.toUpperCase(c));
        }
        enumName = elemName.toString();
    }

    void buildProperty(JavaClassWriter cf, HashMap<String, DataBeanBuilder> beansByInterface) throws IOException {
        cf.println("private " + type.toString() + " " + name + ";");
        DataBeanBuilder bean = beansByInterface.get(type.toString());
        if (bean != null) { // managed bean
            cf.println("private " + bean.keyType + " " + name + "_foreignKey;");
        }
    }

    void buildGetter(JavaClassWriter cf, HashMap<String, DataBeanBuilder> beansByInterface) throws IOException {
        cf.println("");
        cf.startBlock("public " +
                type.toString() + " get" +
                Character.toUpperCase(name.charAt(0)) +
                name.substring(1) + "() {"
        );
        cf.println("return " + name + ";");
        cf.endBlock("}");
        DataBeanBuilder bean = beansByInterface.get(type.toString());
        if (bean != null) { // managed bean
            cf.println("");
            cf.startBlock("public " +
                    bean.keyType + " get" +
                    Character.toUpperCase(name.charAt(0)) +
                    name.substring(1) + "_foreignKey() {"
            );
            cf.println("return " + name + "_foreignKey;");
            cf.endBlock("}");
        }
    }

    void buildSetter(JavaClassWriter cf, HashMap<String, DataBeanBuilder> beansByInterface) throws IOException {
        DataBeanBuilder bean = beansByInterface.get(type.toString());

        cf.println("");
        cf.startBlock("public void set" +
                Character.toUpperCase(name.charAt(0)) +
                name.substring(1) + "(" +
                type.toString() + " " +
                name + ") {"
        );
        cf.println("this." + name + " = " + name + ";");
        if (bean != null) { // managed bean
            cf.startBlock("try {");
            cf.println("this." + name + "_foreignKey = " + name + " == null ? null : (" +
                    bean.keyType + ")" +
                    name + ".getClass().getMethod(\"getDDataBeanKey_\").invoke(" + name + ");");
            cf.endBlock("} catch (Exception e) {;}");
        }
        cf.endBlock("}");

        if (bean != null) { // managed bean
            cf.println("");
            cf.startBlock("public void set" +
                    Character.toUpperCase(name.charAt(0)) +
                    name.substring(1) + "_foreignKey(" +
                    bean.keyType + " " +
                    name + ") {"
            );
            cf.println("this." + name + "_foreignKey = " + name + ";");
            cf.endBlock("}");
        }
    }

    void buildEnumElement(JavaClassWriter cf, HashMap<String, DataBeanBuilder> beansByInterface, ProcessingEnvironment environment) throws IOException {
        TypeMirror typeErasure = environment.getTypeUtils().erasure(collection ?
                ((DeclaredType) type).getTypeArguments().get(0) : type);
        DataBeanBuilder manType = beansByInterface.get(typeErasure.toString());
        if (manType == null) {
            cf.println("/** Value of column " + this.columnName + "*/");
            cf.println(this.enumName + "(\"" +
                    this.columnName + "\",\"" +
                    this.name + "\"," +
                    dataBean.interfaceType + ".class),");
        } else if (!collection) {
            cf.println("/** Value of column " + this.columnName + "*/");
            cf.println(this.enumName + "(\"" +
                    this.columnName + "\",\"" +
                    this.name + "\"," +
                    dataBean.interfaceType + ".class," +
                    manType.interfaceType + ".class),");
        }
    }

    boolean isId() {
        return isId;
    }

    boolean isCollection() {
        return collection;
    }
}
