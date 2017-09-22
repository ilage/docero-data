package org.docero.data.processor;

import org.docero.data.DDataBean;
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
    final String name;
    final TypeMirror interfaceType;
    final TableGrowType grown;
    final DictionaryType dictionary;
    final HashMap<String, DataBeanPropertyBuilder> properties = new HashMap<>();
    final TypeMirror collectionType;
    final String keyType;
    final boolean isKeyComposite;

    DataBeanBuilder(
            Element beanElement, ProcessingEnvironment environment,
            TypeMirror collectionType, TypeMirror mapType
    ) {
        DDataBean ddBean = beanElement.getAnnotation(DDataBean.class);
        schema = (ddBean.schema());
        table = (ddBean.table());
        name = (ddBean.value());
        grown = (ddBean.growth());
        dictionary = (ddBean.dictionary());
        interfaceType = (beanElement.asType());
        TypeMirror voidType = environment.getElementUtils().getTypeElement("java.lang.Void").asType();
        for (Element elt : beanElement.getEnclosedElements())
            if (elt.getKind() == ElementKind.METHOD) {
                DataBeanPropertyBuilder beanBuilder = new DataBeanPropertyBuilder(this,
                        (ExecutableElement) elt, environment, collectionType, mapType, voidType);
                if(beanBuilder.type!=voidType)
                    properties.put(beanBuilder.enumName, beanBuilder);
            }
        this.collectionType = collectionType;
        List<DataBeanPropertyBuilder> ids = properties.values().stream().filter(p -> p.isId).collect(Collectors.toList());
        if (ids.size() == 1) {
            keyType = ids.get(0).type.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) ids.get(0).type).toString() :
                    ids.get(0).type.toString();
            isKeyComposite = false;
        } else {
            keyType = interfaceType + "_Key_";
            isKeyComposite = true;
        }
    }

    String getImplementationName() {
        return interfaceType + "Impl";
    }

    String getTableRef() {
        return (schema==null || schema.length()==0 ? "":"\""+schema+"\".") +
                "\""+table+"\"";
    }

    void build(ProcessingEnvironment environment, HashMap<String, DataBeanBuilder> beansByInterface) throws IOException {
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
            cf.startBlock("public class " +
                    className.substring(simpNameDel + 1)
                    + " implements " + interfaceType + " {");

            for (DataBeanPropertyBuilder property : properties.values()) {
                property.buildProperty(cf, beansByInterface);
            }

            cf.println("");
            if (isKeyComposite) {
                //TODO composite keys
                cf.startBlock("public Object getDDataBeanKey_() {");
                cf.println("return new Object();");
                cf.endBlock("}");
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
                property.buildGetter(cf, beansByInterface);
                property.buildSetter(cf, beansByInterface);
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

            cf.print(interfaceType + "_ value()");
            Optional<DataBeanPropertyBuilder> opt = properties.values().stream().filter(DataBeanPropertyBuilder::isId).findAny();
            if (opt.isPresent()) {
                cf.print(" default " + interfaceType + "_." + opt.get().enumName);
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
}
