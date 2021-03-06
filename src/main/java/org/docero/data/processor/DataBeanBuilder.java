package org.docero.data.processor;

import org.docero.data.*;
import org.docero.data.remote.DDataPrototype;
import org.docero.data.remote.DDataPrototypeId;
import org.docero.dgen.processor.DGenClass;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.xml.bind.annotation.XmlSchema;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
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
    final String keyType;
    final boolean isKeyComposite;
    final TypeMirror versionalType;
    final String inversionalKey;
    final String cacheMap;
    final String discriminatorValue;
    final String xmlNamespace;
    final DataBeanPropertyBuilder discriminatorProperty;
    final boolean abstractBean;
    final boolean prototype;
    private final TypeMirror prototypeExtention;

    static DataBeanBuilder buildEntity(
            TypeElement beanElement, DDataBuilder builder, DataBeanBuilder discriminatedBean
    ) {
        return new DataBeanBuilder(beanElement, builder, discriminatedBean);
    }

    static DataBeanBuilder buildEntity(
            TypeElement beanElement, DDataBuilder builder
    ) {
        return new DataBeanBuilder(beanElement, builder, null);
    }

    static DataBeanBuilder buildPrototype(
            TypeElement beanElement, DDataBuilder builder
    ) {
        return new DataBeanBuilder(beanElement, builder);
    }

    /**
     * build prototype
     */
    private DataBeanBuilder(TypeElement beanElement, DDataBuilder builder) {
        rootBuilder = builder;
        interfaceType = (beanElement.asType());
        prototype = true;
        prototypeExtention = null;
        schema = null;
        table = null;
        name = null;
        grown = TableGrowType.NO;
        dictionary = DictionaryType.NO;
        abstractBean = true;
        cacheMap = new StringBuilder(interfaceType.toString()).reverse().toString();

        for (Element elt : beanElement.getEnclosedElements())
            if (elt.getKind() == ElementKind.METHOD &&
                    !elt.getModifiers().contains(Modifier.DEFAULT) &&
                    !elt.getModifiers().contains(Modifier.STATIC)
            ) {
                DDataPrototypeId pIsId = elt.getAnnotation(DDataPrototypeId.class);
                DataBeanPropertyBuilder beanBuilder = DataBeanPropertyBuilder.from(
                        this, null, pIsId != null,
                        (ExecutableElement) elt, rootBuilder.collectionType, rootBuilder.mapType, rootBuilder.voidType);
                if (beanBuilder.type != rootBuilder.voidType)
                    properties.put(beanBuilder.enumName, beanBuilder);
            }
        discriminatorProperty = null;
        discriminatorValue = null;
        versionalType = null;

        List<DataBeanPropertyBuilder> ids = properties.values().stream()
                .filter(p -> p.isId)
                .collect(Collectors.toList());
        if (ids.size() == 1) {
            isKeyComposite = false;
            keyType = ids.get(0).type.getKind().isPrimitive() ?
                    builder.environment.getTypeUtils().boxedClass((PrimitiveType) ids.get(0).type).toString() :
                    ids.get(0).type.toString();
            inversionalKey = keyType;
        } else {
            isKeyComposite = true;
            keyType = interfaceType + "_Key_";
            inversionalKey = keyType;
        }
        xmlNamespace = null;
    }

    /**
     * build entity
     */
    private DataBeanBuilder(
            TypeElement beanElement, DDataBuilder builder, DataBeanBuilder discriminatedBean
    ) {
        rootBuilder = builder;
        interfaceType = (beanElement.asType());
        prototype = false;

        TypeMirror prototypeInterface = null;
        DGenClass dGen;
        for (TypeMirror typeMirror : beanElement.getInterfaces()) {
            String key = typeMirror.toString();
            if (!key.contains("."))
                key = beanElement.getEnclosingElement().toString() + "." + key;
            dGen = builder.dGenInterface.get(key);
            if (dGen != null) {
                dGen.getProperties().forEach(dGenProperty -> {
                    properties.put(dGenProperty.getName(), DataBeanPropertyBuilder.from(
                            this, dGenProperty, rootBuilder,
                            dGenProperty.getElement().getAnnotation(DDataProperty.class),
                            rootBuilder.collectionType, rootBuilder.mapType
                    ));
                });
                break;
            }
            if (typeMirror.getAnnotation(DDataPrototype.class) != null)
                prototypeInterface = typeMirror;
        }
        prototypeExtention = prototypeInterface;

        DDataBean ddBean = beanElement.getAnnotation(DDataBean.class);
        if (ddBean != null) {
            schema = ddBean.schema();
            table = ddBean.table().trim().length() == 0 ?
                    propertyName2sqlName(interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1)) :
                    ddBean.table();
            name = ddBean.value().trim().length() == 0 ?
                    propertyName2sqlName(interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1)) :
                    ddBean.value().replace(' ', '_');
            grown = (ddBean.growth());
            dictionary = (ddBean.dictionary());
            abstractBean = false;
        } else {
            schema = discriminatedBean.schema;
            table = discriminatedBean.table;
            name = propertyName2sqlName(interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1));
            grown = TableGrowType.NO;
            dictionary = DictionaryType.NO;
            abstractBean = true;
        }
        cacheMap = new StringBuilder(interfaceType.toString()).reverse().toString();

        for (Element elt : beanElement.getEnclosedElements())
            if (elt.getKind() == ElementKind.METHOD &&
                    !elt.getModifiers().contains(Modifier.DEFAULT) &&
                    !elt.getModifiers().contains(Modifier.STATIC)
            ) {
                DDataProperty ddProperty = elt.getAnnotation(DDataProperty.class);
                DataBeanPropertyBuilder beanBuilder = DataBeanPropertyBuilder.from(
                        this, ddProperty, false,
                        (ExecutableElement) elt, rootBuilder.collectionType, rootBuilder.mapType, rootBuilder.voidType);
                if (beanBuilder.type != rootBuilder.voidType)
                    properties.put(beanBuilder.enumName, beanBuilder);
            }

        for (Element elt : builder.environment.getElementUtils().getAllMembers(beanElement))
            if (elt.getKind() == ElementKind.METHOD &&
                    !elt.getModifiers().contains(Modifier.DEFAULT) &&
                    !elt.getModifiers().contains(Modifier.STATIC)
            ) {
                DDataProperty ddProperty = elt.getAnnotation(DDataProperty.class);
                if (ddProperty != null || ((ExecutableElement) elt).getAnnotationMirrors().stream()
                        .anyMatch(a -> a.getAnnotationType().toString().endsWith("_Map_"))) {
                    DataBeanPropertyBuilder beanBuilder = DataBeanPropertyBuilder.from(
                            this, ddProperty, false,
                            (ExecutableElement) elt, rootBuilder.collectionType, rootBuilder.mapType, rootBuilder.voidType);
                    if (!properties.containsKey(beanBuilder.enumName) && beanBuilder.type != rootBuilder.voidType)
                        properties.put(beanBuilder.enumName, beanBuilder);
                }
            }

        List<DataBeanPropertyBuilder> ids = properties.values().stream()
                .filter(p -> p.isId)
                .collect(Collectors.toList());
        if (rootBuilder.environment.getTypeUtils().isSubtype(
                rootBuilder.environment.getTypeUtils().erasure(interfaceType),
                rootBuilder.versionalBeanType)) {
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
        } else
            versionalType = null;

        discriminatorProperty = properties.values().stream()
                .filter(p -> p.isDiscriminator).findAny().orElse(null);
        DDataDiscriminator dataDiscriminator = beanElement.getAnnotation(DDataDiscriminator.class);
        discriminatorValue = dataDiscriminator == null ? null : dataDiscriminator.value();

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

        XmlSchema pkgSchema = beanElement.getEnclosingElement().getAnnotation(XmlSchema.class);
        xmlNamespace = pkgSchema != null ? pkgSchema.namespace() : null;

        // after all properties calculation we build super interface for discriminated bean if needed
        if (discriminatorProperty != null && discriminatedBean == null && beanElement.getInterfaces().size() == 1) {
            String superInterface = beanElement.getInterfaces().get(0).toString();
            if (!builder.beansByInterface.containsKey(superInterface)) {
                TypeElement superType = builder.environment.getElementUtils().getTypeElement(superInterface);
                if (superType != null) builder.beansByInterface.put(superInterface,
                        new DataBeanBuilder(superType, builder, this));
            }
        }
    }

    static String propertyName2sqlName(String value) {
        StringBuilder nameBuilder = new StringBuilder();
        for (char c : value.toCharArray())
            if (nameBuilder.length() == 0)
                nameBuilder.append(Character.toLowerCase(c));
            else if (Character.isUpperCase(c))
                nameBuilder.append('_').append(Character.toLowerCase(c));
            else nameBuilder.append(c);
        return nameBuilder.toString();
    }

    String getImplementationName() {
        return prototype ?
                interfaceType + "Bean" :
                interfaceType + "Impl";
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
            cf.println("@javax.xml.bind.annotation.XmlRootElement(name=\"" +
                    interfaceType.toString().substring(simpNameDel + 1) + "\")");
            cf.println("@javax.xml.bind.annotation.XmlAccessorType(javax.xml.bind.annotation.XmlAccessType.PROPERTY)");
            if (environment.getElementUtils().getTypeElement("com.fasterxml.jackson.annotation.JsonIgnoreProperties") != null)
                cf.println("@com.fasterxml.jackson.annotation.JsonIgnoreProperties({\"handler\",\"ddataBeanKey_\"})");
            if (prototype)
                cf.println("@org.docero.data.remote.DDataPrototypeRealization");
            cf.startBlock("public class " +
                    className.substring(simpNameDel + 1)
                    + (prototype ? "" : " extends " + rootBuilder.basePackage + ".AbstractBean<" + interfaceType + "> ") +
                    " implements " + interfaceType + " {");

            for (DataBeanPropertyBuilder property : properties.values()) {
                property.buildProperty(cf);
            }

            cf.println("");
            if (isKeyComposite) {
                if (!prototype && prototypeExtention == null)
                    cf.println("@javax.xml.bind.annotation.XmlTransient");
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
                        DataBeanPropertyBuilder.printKnownXmlAdapters(cf, actProperty.type);
                        cf.startBlock("public void setActualFrom_(" + actProperty.type + " dt) {");
                        cf.println("" + actProperty.name + " = dt;");
                        cf.endBlock("}");
                    }
                    cf.println("");
                    cf.println("private " + versionalType + " dDataBeanActualAt_;");
                    cf.println("");
                    DataBeanPropertyBuilder.printKnownXmlAdapters(cf, versionalType);
                    cf.println("public " + versionalType + " getDDataBeanActualAt_() {return dDataBeanActualAt_;}");
                    cf.println("");
                    cf.println("public void setDDataBeanActualAt_(" + versionalType + " value) {dDataBeanActualAt_=value;}");
                }
            } else {
                cf.println("@javax.xml.bind.annotation.XmlTransient");
                cf.startBlock("final public " + keyType + " getDDataBeanKey_() {");
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
            cf.println("private int hash_;");
            //cf.println("@javax.xml.bind.annotation.XmlTransient");
            cf.startBlock("final public int hashCode() {");
            if (prototype) {
                cf.startBlock("if(hash_ == 0) {");
                cf.println("int h = 0;");
                for (DataBeanPropertyBuilder p : properties.values())
                    if (p.isId)
                        if (p.type.getKind().isPrimitive())
                            cf.println("h += " +
                                    environment.getTypeUtils().boxedClass((PrimitiveType) p.type).toString() +
                                    ".hashCode(" + p.name + ");"
                            );
                        else cf.println("h += " + p.name + " == null ? 0 : " + p.name + ".hashCode();");
                cf.println("hash_ = h;");
                cf.endBlock("}");
            } else
                cf.println("if(hash_ == 0) hash_ = getDDataBeanKey_().hashCode();");
            cf.println("return hash_;");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("final public boolean equals(Object o) {");
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

            cf.println("");
            cf.startBlock("public int compareTo(" + interfaceType + " o) {");
            cf.println("if (o == null) throw new NullPointerException();");
            boolean notFirst = false;
            for (DataBeanPropertyBuilder property : properties.values())
                if (!(property.isId || property.isVersionFrom)) {
                    notFirst = printCompareBlock(cf, property, notFirst);
                }
            cf.println(notFirst ? "return r;" : "return 0;");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public int compareSimpleTypes(" + interfaceType + " o) {");
            cf.println("if (o == null) throw new NullPointerException();");
            notFirst = false;
            for (DataBeanPropertyBuilder property : properties.values())
                if (!(property.isId || property.isVersionFrom) &&
                        rootBuilder.beansByInterface.get(property.mappedType.toString()) == null
                ) {
                    notFirst = printCompareBlock(cf, property, notFirst);
                }
            cf.println(notFirst ? "return r;" : "return 0;");
            cf.endBlock("}");

            if (prototype) {
                cf.startBlock("private int compare(Object o1, Object o2) {");
                cf.println("if (o1 == null) return o2 == null ? 0 : -1;");
                cf.println("if (o2 == null) return 1;");
                cf.println("if (o1 instanceof java.lang.Comparable)");
                cf.println("  return ((java.lang.Comparable) o1).compareTo(o2);");
                cf.println("else return 0;");
                cf.endBlock("}");
            }

            cf.endBlock("}");
        }
    }

    private boolean printCompareBlock(
            JavaClassWriter cf, DataBeanPropertyBuilder property, boolean notFirst
    ) throws IOException {
        String getter = "get" +
                Character.toUpperCase(property.name.charAt(0)) +
                property.name.substring(1);
        if (notFirst) {
            cf.println("if(r != 0) return r;");
            cf.println("else r = compare(this." +
                    getter + "(), o." + getter + "());");
        } else {
            cf.println("int r = compare(this." +
                    getter + "(), o." + getter + "());");
        }
        return true;
    }

    void buildDataReferenceEnum(
            ProcessingEnvironment environment,
            HashMap<String, Mapping> mappings
    ) throws IOException {
        String className = getImplementationName();
        int simpNameDel = className.lastIndexOf('.');

        try (JavaClassWriter cf = new JavaClassWriter(environment, interfaceType + "_WB_")) {
            final String enumName = interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1) + "_WB_";
            cf.println("package " +
                    className.substring(0, simpNameDel) + ";");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public enum " + enumName + " implements org.docero.data.utils.DDataAttribute {");

            for (DataBeanPropertyBuilder property : properties.values())
                property.buildEnumElementWithBeans(cf, mappings, environment);

            cf.println("NONE_(null,null,null,null,false,false,false,null,null,null,false,true,null,null,null);");
            cf.println("");
            cf.println("public final static String TABLE_NAME = " + (prototype ?
                    "null;" :
                    "\"" + schema + "." + table + "\";"));
            cf.println("public final static Class<" + interfaceType + "> BEAN_INTERFACE = " + interfaceType + ".class;");
            cf.println("public final static " + interfaceType + "_WB_ DISCR_ATTR = " +
                    (discriminatorProperty == null ? "null" : discriminatorProperty.enumName) + ";");
            if (discriminatorValue == null || discriminatorProperty == null)
                cf.println("public final static String DISCR_VAL = null;");
            else
                cf.println("public final static String DISCR_VAL = \"" + discriminatorValue + "\";");
            cf.println("public final static " + interfaceType + "_WB_ VERSION_FROM = " +
                    this.properties.values().stream()
                            .filter(p -> p.isVersionFrom).findAny()
                            .map(p -> p.enumName)
                            .orElse("null") + ";");
            cf.println("public final static " + interfaceType + "_WB_ VERSION_TO = " +
                    this.properties.values().stream()
                            .filter(p -> p.isVersionTo).findAny()
                            .map(p -> p.enumName)
                            .orElse("null") + ";");
            cf.println("");
            cf.println("private final String columnName;");
            cf.println("private final String propertyName;");
            cf.println("private final Class javaType;");
            cf.println("private final Class<? extends java.io.Serializable> interfaceType;");
            cf.println("private final String jdbcType;");
            cf.println("private final boolean dictionary;");
            cf.println("private final boolean mapped;");
            cf.println("private final boolean collection;");
            cf.println("private final String joinTable;");
            cf.println("private final String[] joinBy;");
            cf.println("private final String[] joinOn;");
            cf.println("private final boolean isPrimaryKey;");
            cf.println("private final boolean isNullable;");
            cf.println("private final String readExpression;");
            cf.println("private final String writeExpression;");
            cf.startBlock("private " + enumName + " (String columnName, String propertyName, Class javaType, String jdbcType, boolean dictionary, boolean mapped, boolean collection, String joinTable, String[] joinBy, String[] joinOn, boolean isPrimaryKey, boolean isNullable, Class<? extends java.io.Serializable> interfaceType, String readExpression, String writeExpression) {");
            cf.println("this.columnName = columnName;");
            cf.println("this.propertyName = propertyName;");
            cf.println("this.javaType = javaType;");
            cf.println("this.jdbcType = jdbcType;");
            cf.println("this.dictionary = dictionary;");
            cf.println("this.mapped = mapped;");
            cf.println("this.collection = collection;");
            cf.println("this.joinTable = joinTable;");
            cf.println("this.joinBy = joinBy;");
            cf.println("this.joinOn = joinOn;");
            cf.println("this.isPrimaryKey = isPrimaryKey;");
            cf.println("this.isNullable = isNullable;");
            cf.println("this.interfaceType = interfaceType;");
            cf.println("this.readExpression = readExpression;");
            cf.println("this.writeExpression = writeExpression;");
            cf.endBlock("}");
            cf.println("@Override public String getColumnName() {return columnName;}");
            cf.println("@Override public String getPropertyName() {return propertyName;}");
            cf.println("@Override public Class getJavaType() {return javaType;}");
            cf.println("@Override public String getJdbcType() {return jdbcType;}");
            cf.println("@Override public boolean isDictionary() {return dictionary;}");
            cf.println("@Override public boolean isMappedBean() {return mapped;}");
            cf.println("@Override public boolean isCollection() {return collection;}");
            cf.println("@Override public String joinTable() {return joinTable;}");
            cf.println("@Override public String[] joinBy() {return joinBy;}");
            cf.println("@Override public String[] joinOn() {return joinOn;}");
            cf.println("@Override public boolean isPrimaryKey() {return isPrimaryKey;}");
            cf.println("@Override public boolean isNullable() {return isNullable;}");
            cf.println("@Override public String readExpression() {return readExpression;}");
            cf.println("@Override public String writeExpression() {return writeExpression;}");
            cf.println("@Override public Class<? extends java.io.Serializable> getBeanInterface() {return interfaceType;}");
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
            if (prototype || prototypeExtention == null)
                try (JavaClassWriter cf = new JavaClassWriter(environment, keyType)) {
                    cf.println("package " +
                            className.substring(0, simpNameDel) + ";");
                    cf.startBlock("/*");
                    cf.println("Class generated by docero-data processor.");
                    cf.endBlock("*/");
                    cf.println("@javax.xml.bind.annotation.XmlRootElement(name=\"" +
                            keyType.substring(simpNameDel + 1) + "\")");
                    cf.println("@javax.xml.bind.annotation.XmlAccessorType(javax.xml.bind.annotation.XmlAccessType.PROPERTY)");
                    cf.startBlock("public class " + keyType.substring(simpNameDel + 1) + " implements java.io.Serializable {");
                    List<DataBeanPropertyBuilder> ids = properties.values().stream()
                            .filter(p -> p.isId)
                            .sorted(Comparator.comparing(p -> p.name))
                            .collect(Collectors.toList());
                    for (DataBeanPropertyBuilder property : ids) {
                        cf.println("private final " + property.type + " " + property.name + ";");
                        DataBeanPropertyBuilder.printKnownXmlAdapters(cf, property.type);
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
                    cf.println("private int hash_;");
                    cf.startBlock("public int hashCode() {");
                    cf.println("if(hash_ == 0) hash_ = " + ids.stream().map(p -> {
                                if (p.type.getKind().isPrimitive())
                                    return environment.getTypeUtils().boxedClass((PrimitiveType) p.type) +
                                            ".hashCode(" + p.name + ")";
                                else return "(" + p.name + " == null ? 0 : " + p.name + ".hashCode())";
                    }).collect(Collectors.joining(" ^ ")) + ";");
                    cf.println("return hash_;");
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
            else
                try (JavaClassWriter cf = new JavaClassWriter(environment, keyType)) {
                    cf.println("package " +
                            className.substring(0, simpNameDel) + ";");
                    cf.startBlock("/*");
                    cf.println("Class generated by docero-data processor.");
                    cf.endBlock("*/");
                    cf.startBlock("public class " + keyType.substring(simpNameDel + 1) +
                            " extends " + prototypeExtention + "_Key_ {");
                    cf.endBlock("}");
                }
        /*
            Build Fields Enumeration
        */
        if (name != null)
            try (JavaClassWriter cf = new JavaClassWriter(environment, interfaceType + "_")) {
                final String enumName = interfaceType.toString().substring(interfaceType.toString().lastIndexOf('.') + 1) + "_";
                cf.println("package " +
                        className.substring(0, simpNameDel) + ";");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.startBlock("public enum " + enumName + " implements org.docero.data.utils.DDataBasicAttribute {");

                for (DataBeanPropertyBuilder property : properties.values())
                    property.buildEnumElement(cf, beansByInterface, environment);

                cf.println("NONE_(null,null,null,null,false,false,false,false,true);");
                cf.println("public final static String TABLE_NAME = \"" + schema + "." + table + "\";");
                cf.println("public final static Class<" + interfaceType + "> BEAN_INTERFACE = " + interfaceType + ".class;");
                cf.println("private final String columnName;");
                cf.println("private final String propertyName;");
                cf.println("private final Class javaType;");
                cf.println("private final String jdbcType;");
                cf.println("private final boolean dictionary;");
                cf.println("private final boolean mapped;");
                cf.println("private final boolean collection;");
                cf.println("private final boolean isPrimaryKey;");
                cf.println("private final boolean isNullable;");
                cf.startBlock("private " + enumName + " (String columnName, String propertyName, Class javaType, String jdbcType, boolean dictionary, boolean mapped, boolean collection, boolean isPrimaryKey, boolean isNullable) {");
                cf.println("this.columnName = columnName;");
                cf.println("this.propertyName = propertyName;");
                cf.println("this.javaType = javaType;");
                cf.println("this.jdbcType = jdbcType;");
                cf.println("this.dictionary = dictionary;");
                cf.println("this.mapped = mapped;");
                cf.println("this.collection = collection;");
                cf.println("this.isPrimaryKey = isPrimaryKey;");
                cf.println("this.isNullable = isNullable;");
                cf.endBlock("}");
                cf.println("@Override public String getColumnName() {return columnName;}");
                cf.println("@Override public String getPropertyName() {return propertyName;}");
                cf.println("@Override public Class getJavaType() {return javaType;}");
                cf.println("@Override public String getJdbcType() {return jdbcType;}");
                cf.println("@Override public boolean isDictionary() {return dictionary;}");
                cf.println("@Override public boolean isMappedBean() {return mapped;}");
                cf.println("@Override public boolean isCollection() {return collection;}");
                cf.println("@Override public boolean isPrimaryKey() {return isPrimaryKey;}");
                cf.println("@Override public boolean isNullable() {return isNullable;}");
                cf.endBlock("}");
            }
        /*
            Build Mapping Annotation
        */
        if (name != null)
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
                cf.println("@Target({ElementType.METHOD,ElementType.FIELD})");
                cf.startBlock("public @interface " + annotName + " {");
                cf.println("/** Bean attribute used for mapping */");
                cf.print(interfaceType + "_[] value()");
                Optional<DataBeanPropertyBuilder> opt = properties.values().stream().filter(DataBeanPropertyBuilder::isId)
                        .filter(p -> !p.isVersionFrom).findAny();
                if (opt.isPresent()) {
                    cf.print(" default {" + interfaceType + "_." + opt.get().enumName + "}");
                }
                cf.println(";");

                cf.println("/** Use lazy load option in any method of data repositories */");
                cf.println("boolean alwaysLazy() default false;");
                cf.println("/** Mark implementation property as transient " +
                        "(for ObjectOutputStream and XML serialization)" +
                        "<p>Do not forget mark method with @JsonIgnore annotation if you don't really want it.</p>" +
                        "*/");
                cf.println("boolean markTransient() default false;");
                cf.println("/** Function name used for remote beans mapping*/");
                cf.println("String func() default \"\";");

                for (DataBeanPropertyBuilder property : properties.values()) {
                    if (property.isCollection()) {
                        List<? extends TypeMirror> typeParams =
                                ((DeclaredType) property.type).getTypeArguments();
                        if (typeParams.size() == 1) {
                            TypeMirror et = environment.getTypeUtils().erasure(typeParams.get(0));
                            DataBeanBuilder manType = beansByInterface.get(et.toString());
                            if (manType != null) {
                                cf.println("/** */");
                                cf.println(manType.interfaceType + "_[] " + property.name + "() default {};");
                            }
                        }
                    } else {
                        DataBeanBuilder manType = beansByInterface.get(property.type.toString());
                        if (manType != null) {
                            cf.println("/** */");
                            cf.println(manType.interfaceType + "_[] " + property.name + "() default {};");
                        }
                    }
                }

                cf.endBlock("}");
            }
    }

    static String dateNowFrom(TypeMirror versionalType) {
        if ("java.time.LocalDateTime".equals(versionalType.toString()))
            return "org.docero.data.utils.DMLOperations.localDateTime()";
        else if ("java.time.LocalDate".equals(versionalType.toString()))
            return "org.docero.data.utils.DMLOperations.localDate()";
        else if ("java.util.Date".equals(versionalType.toString()))
            return "org.docero.data.utils.DMLOperations.date()";
        else if ("java.sql.Timestamp".equals(versionalType.toString()))
            return "org.docero.data.utils.DMLOperations.timestamp()";
        return null;
    }

    boolean isDictionary() {
        return dictionary != DictionaryType.NO;
    }
}
