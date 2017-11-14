package org.docero.data.processor;

import org.docero.data.DictionaryType;
import org.docero.data.SelectId;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

class DDataMethodBuilder {
    final TypeMirror returnType;
    final String methodName;
    final ArrayList<DDataMethodParameter> parameters = new ArrayList<>();
    final List<? extends TypeMirror> throwTypes;
    final DataRepositoryBuilder repositoryBuilder;
    final long methodIndex;
    final MType methodType;
    final boolean returnSimpleType;
    final String selectId;

    DDataMethodBuilder(DataRepositoryBuilder repositoryBuilder, ExecutableElement methodElement) {
        this.repositoryBuilder = repositoryBuilder;
        returnType = methodElement.getReturnType();
        methodName = methodElement.getSimpleName().toString();
        methodIndex = repositoryBuilder.methods.stream().filter(m -> methodName.equals(m.methodName)).count();
        for (VariableElement variableElement : methodElement.getParameters()) {
            parameters.add(new DDataMethodParameter(variableElement.getSimpleName(), variableElement.asType()));
        }
        throwTypes = methodElement.getThrownTypes();
        if (returnType == null) {
            String nameString = methodName.toLowerCase();
            this.methodType = nameString.contains("insert") ? MType.INSERT : (
                    nameString.contains("update") ? MType.UPDATE :
                            MType.DELETE
            );
            returnSimpleType = false;
        } else {
            String ttype = returnType.toString();
            if (ttype.contains("java.util.List")) methodType = MType.SELECT;
            else if (ttype.contains("java.util.Map")) methodType = MType.SELECT;
            else methodType = MType.GET;
            returnSimpleType = returnType.getKind().isPrimitive() ||
                    "java.lang.Long".equals(returnType.toString()) ||
                    "java.math.BigInteger".equals(returnType.toString());
        }
        SelectId select = methodElement.getAnnotation(SelectId.class);
        selectId = select != null ? select.value() : null;
    }

    DDataMethodBuilder(DataRepositoryBuilder repositoryBuilder, DDataMethodBuilder.MType methodType) {
        this.repositoryBuilder = repositoryBuilder;
        returnType = repositoryBuilder.forInterfaceName;
        returnSimpleType = false;
        this.methodType = methodType;
        methodIndex = 0;
        throwTypes = Collections.emptyList();
        switch (methodType) {
            case GET:
                methodName = "get";
                parameters.add(new DDataMethodParameter("id", repositoryBuilder.idClass));
                break;
            case INSERT:
                methodName = "insert";
                parameters.add(new DDataMethodParameter("bean", repositoryBuilder.forInterfaceName));
                break;
            case UPDATE:
                methodName = "update";
                parameters.add(new DDataMethodParameter("bean", repositoryBuilder.forInterfaceName));
                break;
            default:
                methodName = "delete";
                parameters.add(new DDataMethodParameter("id", repositoryBuilder.idClass));
        }
        selectId = null;
    }

    void build(JavaClassWriter cf) throws IOException {
        String throwsPart = throwTypes.size() > 0 ?
                "throws " + throwTypes.stream().map(TypeMirror::toString).collect(Collectors.joining(", ")) :
                "";
        cf.println("");
        DataBeanBuilder bean = repositoryBuilder.rootBuilder.beansByInterface.get(repositoryBuilder.forInterfaceName());

        if (bean.dictionary != DictionaryType.NO && repositoryBuilder.rootBuilder.useSpringCache) {
            if (repositoryBuilder.defaultGetMethod == this)
                cf.println("@org.springframework.cache.annotation.Cacheable(cacheNames=\"" + bean.cacheMap +
                        "\", sync=true)");
            else if (repositoryBuilder.defaultDeleteMethod == this)
                cf.println("@org.springframework.cache.annotation.CacheEvict(cacheNames=\"" + bean.cacheMap +
                        "\")");
        }
        cf.startBlock("public " + (returnType == null ? "void" : returnType) + " " + methodName + "(");
        int i = 0;
        for (DDataMethodParameter parameter : parameters) {
            cf.print("" + parameter.type + " " + parameter.name);
            if (++i < parameters.size()) cf.println(",");
            else cf.println("");
        }
        cf.println(")" + throwsPart + " {");

        if (returnType != null) {
            cf.print("return getSqlSession().");
            if (returnType.toString().contains("java.util.List")) {
                cf.print("selectList");
            } else if (returnType.toString().contains("java.util.Map")) {
                cf.print("selectMap");
            } else {
                cf.print("selectOne");
            }
        } else {
            switch (methodType) {
                case INSERT:
                    cf.print("insert");
                    break;
                case UPDATE:
                    cf.print("update");
                    break;
                default:
                    cf.print("delete");
            }
        }
        if (selectId != null)
            cf.print("(\"" + selectId + "\"");
        else
            cf.print("(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                repositoryBuilder.defaultGetMethod == this ? "" : "_" + methodIndex) + "\"");

        if (parameters.size() > 0) {
            if (bean.isKeyComposite && parameters.size() == 1 && (
                    repositoryBuilder.defaultGetMethod == this || repositoryBuilder.defaultDeleteMethod == this
            )) {
                cf.startBlock(", ");
                cf.startBlock("new java.util.HashMap<java.lang.String, java.lang.Object>(){{");
                for (DataBeanPropertyBuilder property : bean.properties.values())
                    if (property.isId) {
                        cf.println("this.put(\"" + property.name + "\", " + parameters.get(0).name + ".get" +
                                Character.toUpperCase(property.name.charAt(0)) +
                                property.name.substring(1) + "());");
                    }
                cf.endBlock("}}");
                cf.endBlock(");");
            } else if (parameters.size() == 1 && parameters.get(0).type.equals(repositoryBuilder.forInterfaceName)) {
                cf.println(", " + parameters.get(0).name + ");");
            } else {
                cf.startBlock(", ");
                cf.startBlock("new java.util.HashMap<java.lang.String, java.lang.Object>(){{");
                for (DDataMethodParameter parameter : parameters) {
                    cf.println("this.put(\"" + parameter.name + "\", " + parameter.name + ");");
                }
                cf.endBlock("}}");
                cf.endBlock(");");
            }
        } else {
            cf.println(");");
        }

        cf.endBlock("}");

        if (repositoryBuilder.defaultGetMethod == this && repositoryBuilder.versionalType != null) {
            cf.println("");
            if (throwTypes.size() != 0) {
                cf.print("throws ");
                i = 0;
                for (TypeMirror throwType : throwTypes) {
                    cf.print(throwType.toString());
                    if (++i < throwTypes.size()) cf.print(", ");
                    else cf.print(" ");
                }
            }
            cf.startBlock("public " + returnType + " get(" +
                    repositoryBuilder.idClass + " id)" + throwsPart + " {");
            cf.println("return get(id, " +
                    DataBeanBuilder.dateNowFrom(repositoryBuilder.versionalType) +
                    ");");
            cf.endBlock("}");
        }
    }

    enum MType {SELECT, GET, INSERT, UPDATE, DELETE}

    static class DDataMethodParameter {
        final String name;
        final TypeMirror type;

        DDataMethodParameter(Name simpleName, TypeMirror typeMirror) {
            this.name = simpleName.toString();
            this.type = typeMirror;
        }

        DDataMethodParameter(String simpleName, TypeMirror typeMirror) {
            this.name = simpleName;
            this.type = typeMirror;
        }
    }
}
