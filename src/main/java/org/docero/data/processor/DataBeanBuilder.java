package org.docero.data.processor;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;
import org.docero.data.DictionaryType;
import org.docero.data.TableGrowType;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class DataBeanBuilder {
    private final String schema;
    private final String table;
    final DDataBuilder rootBuilder;
    final String name;
    final TypeMirror interfaceType;
    final TableGrowType grown;
    final DictionaryType dictionary;
    final HashMap<String, DataBeanPropertyBuilder> properties = new HashMap<>();
    final TypeMirror collectionType;
    final String keyType;
    final boolean isKeyComposite;
    final TypeMirror versionalType;
    final String inversionalKey;

    DataBeanBuilder(
            Element beanElement, DDataBuilder builder,
            TypeMirror collectionType, TypeMirror mapType, TypeMirror versionedBeanType
    ) {
        rootBuilder = builder;
        DDataBean ddBean = beanElement.getAnnotation(DDataBean.class);
        schema = (ddBean.schema());
        table = (ddBean.table());
        name = (ddBean.value());
        grown = (ddBean.growth());
        dictionary = (ddBean.dictionary());
        interfaceType = (beanElement.asType());
        TypeMirror voidType = builder.environment.getElementUtils().getTypeElement("java.lang.Void").asType();

        for (Element elt : beanElement.getEnclosedElements())
            if (elt.getKind() == ElementKind.METHOD &&
                    !elt.getModifiers().contains(Modifier.DEFAULT) &&
                    !elt.getModifiers().contains(Modifier.STATIC)
                    ) {
                DDataProperty ddProperty = elt.getAnnotation(DDataProperty.class);
                DataBeanPropertyBuilder beanBuilder = new DataBeanPropertyBuilder(this, ddProperty,
                        (ExecutableElement) elt, builder.environment, collectionType, mapType, voidType);
                if (beanBuilder.type != voidType)
                    properties.put(beanBuilder.enumName, beanBuilder);
            }

        for (Element elt : builder.environment.getElementUtils().getAllMembers((TypeElement) beanElement))
            if (elt.getKind() == ElementKind.METHOD &&
                    !elt.getModifiers().contains(Modifier.DEFAULT) &&
                    !elt.getModifiers().contains(Modifier.STATIC)
                    ) {
                DDataProperty ddProperty = elt.getAnnotation(DDataProperty.class);
                if (ddProperty != null || ((ExecutableElement)elt).getAnnotationMirrors().stream()
                        .anyMatch(a->a.getAnnotationType().toString().endsWith("_Map_"))) {
                    DataBeanPropertyBuilder beanBuilder = new DataBeanPropertyBuilder(this, ddProperty,
                            (ExecutableElement) elt, builder.environment, collectionType, mapType, voidType);
                    if (!properties.containsKey(beanBuilder.enumName) && beanBuilder.type != voidType)
                        properties.put(beanBuilder.enumName, beanBuilder);
                }
            }

        this.collectionType = collectionType;
        List<DataBeanPropertyBuilder> ids = properties.values().stream()
                .filter(p -> p.isId)
                .collect(Collectors.toList());
        if (rootBuilder.environment.getTypeUtils().isSubtype(
                rootBuilder.environment.getTypeUtils().erasure(interfaceType),
                versionedBeanType)) {
            if (interfaceType.toString().startsWith("org.docero.data.DDataVersionalBean"))
                versionalType = ((DeclaredType) interfaceType).getTypeArguments().get(0);
            else {
                List<? extends TypeMirror> directSupertypes = rootBuilder.environment.getTypeUtils().directSupertypes(interfaceType);
                TypeMirror vt = directSupertypes.stream()
                        .filter(ta -> ta.toString().startsWith("org.docero.data.DDataVersionalBean"))
                        .findAny()
                        .map(t -> ((DeclaredType) t).getTypeArguments().get(0))
                        .orElse(null);
                vt = vt != null ? vt : directSupertypes.stream()
                        .flatMap(dst -> rootBuilder.environment.getTypeUtils().directSupertypes(dst).stream())
                        .filter(ta -> ta.toString().startsWith("org.docero.data.DDataVersionalBean"))
                        .findAny()
                        .map(t -> ((DeclaredType) t).getTypeArguments().get(0))
                        .orElse(null);
                vt = vt != null ? vt : directSupertypes.stream()
                        .flatMap(dst -> rootBuilder.environment.getTypeUtils().directSupertypes(dst).stream())
                        .flatMap(dst -> rootBuilder.environment.getTypeUtils().directSupertypes(dst).stream())
                        .filter(ta -> ta.toString().startsWith("org.docero.data.DDataVersionalBean"))
                        .findAny()
                        .map(t -> ((DeclaredType) t).getTypeArguments().get(0))
                        .orElse(null);
                versionalType = vt;
            }
        } else
            versionalType = null;

        if (ids.size() == 1) {
            keyType = ids.get(0).type.getKind().isPrimitive() ?
                    builder.environment.getTypeUtils().boxedClass((PrimitiveType) ids.get(0).type).toString() :
                    ids.get(0).type.toString();
            isKeyComposite = false;
            inversionalKey = keyType;
        } else {
            isKeyComposite = true;
            List<DataBeanPropertyBuilder> iids = properties.values().stream()
                    .filter(p -> p.isId && !p.isVersionFrom)
                    .collect(Collectors.toList());
            if (versionalType != null) {
                keyType = interfaceType + "_HKey_";
                if (iids.size() == 1)
                    inversionalKey = iids.get(0).type.getKind().isPrimitive() ?
                            builder.environment.getTypeUtils().boxedClass((PrimitiveType) iids.get(0).type).toString() :
                            iids.get(0).type.toString();
                else
                    inversionalKey = interfaceType + "_Key_";
            } else {
                keyType = interfaceType + "_Key_";
                inversionalKey = keyType;
            }
        }
    }

    String getImplementationName() {
        return interfaceType + "Impl";
    }

    String getTableRef() {
        return (schema == null || schema.length() == 0 ? "" : "\"" + schema + "\".") +
                "\"" + table + "\"";
    }

    void buildImplementation(ProcessingEnvironment environment) throws IOException {
        /*
            Build Implementation
        */
        String className = getImplementationName();
        int simpNameDel = className.lastIndexOf('.');

        try (JavaClassWriter cf = new JavaClassWriter(environment, className)) {
            cf.println("package " +
                    className.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            if (environment.getElementUtils().getTypeElement("com.fasterxml.jackson.annotation.JsonIgnoreProperties") != null)
                cf.println("@com.fasterxml.jackson.annotation.JsonIgnoreProperties({\"handler\",\"ddataBeanKey_\"})");
            cf.startBlock("public class " +
                    className.substring(simpNameDel + 1)
                    + " implements " + interfaceType + " {");

            for (DataBeanPropertyBuilder property : properties.values()) {
                property.buildProperty(cf);
            }

            cf.println("");
            if (isKeyComposite) {
                cf.startBlock("public " + keyType + " getDDataBeanKey_() {");
                cf.println("return new " + keyType + "(" +
                        properties.values().stream()
                                .filter(p -> p.isId)
                                .map(p -> p.name).sorted()
                                .collect(Collectors.joining(", ")) +
                        ");");
                cf.endBlock("}");
                if (versionalType != null) {
                    DataBeanPropertyBuilder actProperty =
                            properties.values().stream().filter(p -> p.isVersionFrom).findAny().orElse(null);
                    if (actProperty != null) {
                        cf.println("");
                        cf.startBlock("public " + actProperty.type + " getActualFrom_() {");
                        cf.println("return " + actProperty.name + ";");
                        cf.endBlock("}");
                        cf.println("");
                        cf.startBlock("public void setActualFrom_(" + actProperty.type + " dt) {");
                        cf.println("" + actProperty.name + " = dt;");
                        cf.endBlock("}");
                    }
                    cf.println("");
                    cf.println("private " + versionalType + " dDataBeanActualAt_;");
                    cf.println("");
                    cf.println("public " + versionalType + " getDDataBeanActualAt_() {return dDataBeanActualAt_;}");
                    cf.println("");
                    cf.println("public void setDDataBeanActualAt_(" + versionalType + " value) {dDataBeanActualAt_=value;}");
                }
            } else {
                cf.startBlock("public " + keyType + " getDDataBeanKey_() {");
                Optional<DataBeanPropertyBuilder> idPropOpt = properties.values().stream().filter(p -> p.isId).findAny();
                if (idPropOpt.isPresent())
                    cf.println("return " + idPropOpt.get().name + ";");
                else
                    cf.println("return null;");
                cf.endBlock("}");
            }

            for (DataBeanPropertyBuilder property : properties.values()) {
                property.buildGetter(cf);
                property.buildSetter(cf);
            }

            cf.println("");
            cf.startBlock("public int hashCode() {");
            cf.println("return getDDataBeanKey_().hashCode();");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public boolean equals(Object o) {");
            cf.println("return o!=null && o instanceof " + interfaceType + " && " +
                    properties.values().stream()
                            .filter(DataBeanPropertyBuilder::isId)
                            .map(p -> {
                                String propertyGetter = "((" + interfaceType + ")o).get" +
                                        Character.toUpperCase(p.name.charAt(0)) + p.name.substring(1) + "()";
                                if (p.type.getKind().isPrimitive()) return p.name + "==" + propertyGetter;
                                else return "((" + p.name + "==null && " + propertyGetter + "==null )||(" +
                                        p.name + "!=null && " + p.name + ".equals(" + propertyGetter + ")" + "))";
                            }).collect(Collectors.joining(" && ")) +
                    ";");
            cf.endBlock("}");

            cf.endBlock("}");
        }
    }

    void buildAnnotationsAndEnums(ProcessingEnvironment environment, HashMap<String, DataBeanBuilder> beansByInterface) throws IOException {
        /*
            Build Implementation
        */
        String className = getImplementationName();
        int simpNameDel = className.lastIndexOf('.');

        if (versionalType != null && inversionalKey.endsWith("_"))
            try (JavaClassWriter cf = new JavaClassWriter(environment, inversionalKey)) {
                cf.println("package " +
                        className.substring(0, simpNameDel) + ";");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.startBlock("public class " + inversionalKey.substring(simpNameDel + 1) + " implements java.io.Serializable {");
                List<DataBeanPropertyBuilder> ids = properties.values().stream()
                        .filter(p -> p.isId && !p.isVersionFrom)
                        .sorted(Comparator.comparing(p -> p.name))
                        .collect(Collectors.toList());
                for (DataBeanPropertyBuilder property : ids) {
                    cf.println("private final " + property.type + " " + property.name + ";");
                    cf.println("public " + property.type + " get" +
                            Character.toUpperCase(property.name.charAt(0)) + property.name.substring(1) +
                            "() { return " + property.name + "; }");
                    cf.println("");
                }
                cf.startBlock("public " + inversionalKey.substring(simpNameDel + 1) + "(" +
                        ids.stream().map(p -> p.type + " " + p.name)
                                .collect(Collectors.joining(",")) + ") {");
                for (DataBeanPropertyBuilder property : ids)
                    cf.println("this." + property.name + " = " + property.name + ";");
                cf.endBlock("}");

                cf.endBlock("}");
            }

        if (isKeyComposite)
            try (JavaClassWriter cf = new JavaClassWriter(environment, keyType)) {
                cf.println("package " +
                        className.substring(0, simpNameDel) + ";");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.startBlock("public class " + keyType.substring(simpNameDel + 1) + " implements java.io.Serializable {");
                List<DataBeanPropertyBuilder> ids = properties.values().stream()
                        .filter(p -> p.isId)
                        .sorted(Comparator.comparing(p -> p.name))
                        .collect(Collectors.toList());
                for (DataBeanPropertyBuilder property : ids) {
                    cf.println("private final " + property.type + " " + property.name + ";");
                    cf.println("public " + property.type + " get" +
                            Character.toUpperCase(property.name.charAt(0)) + property.name.substring(1) +
                            "() { return " + property.name + "; }");
                    cf.println("");
                }
                cf.startBlock("public " + keyType.substring(simpNameDel + 1) + "(" +
                        ids.stream().map(p -> p.type + " " + p.name)
                                .collect(Collectors.joining(",")) + ") {");
                for (DataBeanPropertyBuilder property : ids)
                    cf.println("this." + property.name + " = " + property.name + ";");
                cf.endBlock("}");

                cf.println("");
                cf.startBlock("public int hashCode() {");
                cf.println("return " + ids.stream().map(p -> {
                    if (p.type.getKind().isPrimitive()) return "(int)" + p.name;
                    else return "(" + p.name + "==null?0:" + p.name + ".hashCode())";
                }).collect(Collectors.joining(" ^ ")) + ";");
                cf.endBlock("}");

                cf.println("");
                cf.startBlock("public boolean equals(Object o) {");
                cf.println("return o!=null && o instanceof " + keyType + " && " +
                        ids.stream().map(p -> {
                            if (p.type.getKind().isPrimitive()) return p.name + "==((" + keyType + ")o)." + p.name;
                            else return "((" + p.name + "==null && ((" + keyType + ")o)." + p.name + "==null )||(" +
                                    p.name + "!=null && " + p.name + ".equals(o)" + "))";
                        }).collect(Collectors.joining(" && ")) +
                        ";");
                cf.endBlock("}");

                if (versionalType != null) {
                    if (!ids.get(ids.size() - 1).isVersionFrom) {
                        cf.println("");
                        cf.startBlock("public " + keyType.substring(simpNameDel + 1) + "(" +
                                inversionalKey + " key, " +
                                versionalType + " at) {");
                        for (DataBeanPropertyBuilder property : ids)
                            if (!property.isVersionFrom)
                                cf.println("this." + property.name + " = key" +
                                        (inversionalKey.endsWith("_") ? ".get" +
                                                Character.toUpperCase(property.name.charAt(0)) + property.name.substring(1) +
                                                "()" :
                                                "") +
                                        ";");
                        cf.println(ids.stream().filter(p -> p.isVersionFrom).findAny().map(p ->
                                "this." + p.name + " = at;").orElse(""));
                        cf.endBlock("}");
                    }
                    cf.println("");
                    cf.startBlock("public " + keyType.substring(simpNameDel + 1) + "(" +
                            inversionalKey + " key) {");
                    for (DataBeanPropertyBuilder property : ids)
                        if (!property.isVersionFrom)
                            cf.println("this." + property.name + " = key" +
                                    (inversionalKey.endsWith("_") ? ".get" +
                                            Character.toUpperCase(property.name.charAt(0)) + property.name.substring(1) +
                                            "()" :
                                            "") +
                                    ";");
                    cf.println(ids.stream().filter(p -> p.isVersionFrom).findAny().map(p ->
                            "this." + p.name + " = " + dateNowFrom(versionalType) + ";").orElse(""));
                    cf.endBlock("}");
                }

                cf.endBlock("}");
            }

        /*
            Build Fields Enumeration
        */
        try (JavaClassWriter cf = new JavaClassWriter(environment, interfaceType + "_")) {
            final String enumName = interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1) + "_";
            cf.println("package " +
                    className.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public enum " + enumName + " implements org.docero.data.utils.DDataAttribute {");

            for (DataBeanPropertyBuilder property : properties.values())
                property.buildEnumElement(cf, beansByInterface, environment);

            cf.println("NONE_(null,null);");
            cf.println("private final String columnName;");
            cf.println("private final String propertyName;");
            cf.println("private final Class[] beanInterface;");
            cf.startBlock("private " + enumName + " (String columnName, String propertyName, Class... beanInterface) {");
            cf.println("this.columnName = columnName;");
            cf.println("this.propertyName = propertyName;");
            cf.println("this.beanInterface = beanInterface;");
            cf.endBlock("}");
            cf.println("@Override public String getColumnName() {return columnName;}");
            cf.println("@Override public String getPropertyName() {return propertyName;}");
            cf.println("@Override public Class[] getInterface() {return beanInterface;}");

            cf.endBlock("}");
        }
        /*
            Build Mapping Annotation
        */
        try (JavaClassWriter cf = new JavaClassWriter(environment, interfaceType + "_Map_")) {
            final String annotName = interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1) + "_Map_";
            cf.println("package " +
                    className.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.println("import java.lang.annotation.ElementType;");
            cf.println("import java.lang.annotation.Retention;");
            cf.println("import java.lang.annotation.RetentionPolicy;");
            cf.println("import java.lang.annotation.Target;");
            cf.println("@Retention(RetentionPolicy.SOURCE)");
            cf.println("@Target(ElementType.METHOD)");
            cf.startBlock("public @interface " + annotName + " {");

            cf.print(interfaceType + "_[] value()");
            Optional<DataBeanPropertyBuilder> opt = properties.values().stream().filter(DataBeanPropertyBuilder::isId).findAny();
            if (opt.isPresent()) {
                cf.print(" default {" + interfaceType + "_." + opt.get().enumName + "}");
            }
            cf.println(";");

            for (DataBeanPropertyBuilder property : properties.values()) {
                if (property.isCollection()) {
                    List<? extends TypeMirror> typeParams =
                            ((DeclaredType) property.type).getTypeArguments();
                    if (typeParams.size() == 1) {
                        TypeMirror et = environment.getTypeUtils().erasure(typeParams.get(0));
                        DataBeanBuilder manType = beansByInterface.get(et.toString());
                        if (manType != null) {
                            cf.println(manType.interfaceType + "_[] " + property.name + "() default {};");
                        }
                    }
                } else {
                    DataBeanBuilder manType = beansByInterface.get(property.type.toString());
                    if (manType != null) {
                        cf.println(manType.interfaceType + "_[] " + property.name + "() default {};");
                    }
                }
            }

            cf.endBlock("}");
        }
    }

    static String dateNowFrom(TypeMirror versionalType) {
        if ("java.time.LocalDateTime".equals(versionalType.toString()))
            return "java.time.LocalDateTime.now()";
        else if ("java.time.LocalDate".equals(versionalType.toString()))
            return "java.time.LocalDate.now()";
        else if ("java.util.Date".equals(versionalType.toString()))
            return "new java.util.Date()";
        else if ("java.sql.Timestamp".equals(versionalType.toString()))
            return "new java.sql.Timestamp(System.currentTimeMillis())";
        return null;
    }
}
