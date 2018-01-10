package org.docero.data.processor;

import org.docero.data.DictionaryType;
import org.docero.data.SelectId;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class DDataMethodBuilder {
    private static final String ROW_BOUNDS_CLASS = "org.apache.ibatis.session.RowBounds";

    final List<? extends TypeVariable> typeVariables;
    final TypeMirror returnType;
    final String methodName;
    final ArrayList<DDataMethodParameter> parameters = new ArrayList<>();
    final List<? extends TypeMirror> throwTypes;
    final DataRepositoryBuilder repositoryBuilder;
    final long methodIndex;
    final MType methodType;
    final boolean returnSimpleType;
    final String selectId;
    private List<DDataMapBuilder.FilterOption> filters = Collections.emptyList();
    private VariableElement order;

    DDataMethodBuilder(DataRepositoryBuilder repositoryBuilder, ExecutableElement methodElement) {
        this.repositoryBuilder = repositoryBuilder;
        returnType = methodElement.getReturnType();
        methodName = methodElement.getSimpleName().toString();
        boolean returnNothing = returnType == null || returnType.getKind() == TypeKind.VOID;
        for (VariableElement variableElement : methodElement.getParameters()) {
            parameters.add(new DDataMethodParameter(variableElement.getSimpleName(), variableElement.asType()));
        }
        throwTypes = methodElement.getThrownTypes();
        typeVariables = ((ExecutableType) methodElement.asType()).getTypeVariables();
        if (returnNothing) {
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
        boolean defaultMethod = ("get".equals(methodName) && methodElement.getParameters().size() == 1) || (
                ("update".equals(methodName) || "insert".equals(methodName) || "delete".equals(methodName)
                ) && returnNothing && methodElement.getParameters().size() == 1);
        methodIndex = defaultMethod ? 0 : repositoryBuilder.methods.stream()
                .filter(m -> methodName.equals(m.methodName)).count() + 1;
        SelectId select = methodElement.getAnnotation(SelectId.class);
        selectId = select != null ? select.value() : null;
    }

    DDataMethodBuilder(
            DataRepositoryBuilder repositoryBuilder,
            MType methodType
    ) {
        this.repositoryBuilder = repositoryBuilder;
        typeVariables = Collections.emptyList();
        returnSimpleType = false;
        this.methodType = methodType;
        methodIndex = 0;
        throwTypes = Collections.emptyList();
        switch (methodType) {
            case GET:
                methodName = "get";
                returnType = repositoryBuilder.forInterfaceName;
                parameters.add(new DDataMethodParameter("id", repositoryBuilder.idClass));
                break;
            case INSERT:
                methodName = "insert";
                returnType = null;
                parameters.add(new DDataMethodParameter("bean", repositoryBuilder.forInterfaceName));
                break;
            case UPDATE:
                methodName = "update";
                returnType = null;
                parameters.add(new DDataMethodParameter("bean", repositoryBuilder.forInterfaceName));
                break;
            default:
                methodName = "delete";
                returnType = null;
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
        cf.startBlock("public " + typeVariables2String() +
                (returnType == null ? "void" : returnType) + " " + methodName + "(");
        int i = 0;
        for (DDataMethodParameter parameter : parameters) {
            cf.print("" + parameter.type + " " + parameter.name);
            if (++i < parameters.size()) cf.println(",");
            else cf.println("");
        }
        cf.endBlock(") " + throwsPart);
        cf.startBlock("{");

        switch (methodType) {
            case GET:
                cf.print("return getSqlSession().");
                cf.print("selectOne");
                break;
            case SELECT:
                if (returnType != null) {
                    cf.print("return getSqlSession().");
                    if (returnType.toString().contains("java.util.List")) {
                        cf.print("selectList");
                    } else if (returnType.toString().contains("java.util.Map")) {
                        cf.print("selectMap");
                    } else {
                        cf.print("selectOne");
                    }
                }
                break;
            case INSERT:
                String beanParameterName = parameters.get(0).name;
                if (repositoryBuilder.discriminator != null)
                    for (DataRepositoryDiscriminator.Item item : repositoryBuilder.discriminator.beans) {
                        DataRepositoryBuilder strep = repositoryBuilder.rootBuilder.repositoriesByBean.get(item.beanInterface);
                        cf.startBlock("if (" + beanParameterName + " instanceof " + item.beanInterface + ") {");
                        cf.println("getSqlSession().insert(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                                methodIndex == 0 ? "" : "_" + methodIndex) + "_" +
                                item.beanInterfaceShort() + "\", " + beanParameterName + ");");
                        cf.println("return;");
                        cf.endBlock("}");
                    }
                cf.print("getSqlSession().insert");
                break;
            case UPDATE:
                beanParameterName = parameters.get(0).name;
                if (repositoryBuilder.discriminator != null)
                    for (DataRepositoryDiscriminator.Item item : repositoryBuilder.discriminator.beans) {
                        DataRepositoryBuilder strep = repositoryBuilder.rootBuilder.repositoriesByBean.get(item.beanInterface);
                        cf.startBlock("if (" + beanParameterName + " instanceof " + item.beanInterface + ") {");
                        cf.println("getSqlSession().update(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                                methodIndex == 0 ? "" : "_" + methodIndex) + "_" +
                                item.beanInterfaceShort() + "\", " + beanParameterName + ");");
                        cf.println("return;");
                        cf.endBlock("}");
                    }
                cf.print("getSqlSession().update");
                break;
            default:
                cf.print("getSqlSession().delete");
        }
        if (selectId != null)
            cf.print("(\"" + selectId + "\"");
        else
            cf.print("(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                    methodIndex == 0 ? "" : "_" + methodIndex) + "\"");

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
            } else if (parameters.size() == 1 && parameters.get(0).type.equals(repositoryBuilder.forInterfaceName)) {
                cf.print(", " + parameters.get(0).name);
            } else {
                cf.startBlock(", ");
                cf.startBlock("new java.util.HashMap<java.lang.String, java.lang.Object>(){{");
                for (DDataMethodParameter parameter : parameters) {
                    DDataMapBuilder.FilterOption filter = filters.stream()
                            .filter(f -> f.parameter != null && f.parameter.equals(parameter.name))
                            .findAny().orElse(null);
                    String parameterFunc = parameter.name;
                    if (filter != null) {
                        switch (filter.option) {
                            case LIKE:
                                parameterFunc = "org.docero.data.utils.DDataLike.in(" + parameter.name + ")";
                                break;
                            case LIKE_HAS:
                                parameterFunc = "org.docero.data.utils.DDataLike.has(" + parameter.name + ")";
                                break;
                            case LIKE_ENDS:
                                parameterFunc = "org.docero.data.utils.DDataLike.ends(" + parameter.name + ")";
                                break;
                            case LIKE_STARTS:
                                parameterFunc = "org.docero.data.utils.DDataLike.starts(" + parameter.name + ")";
                                break;
                            case LIKE_ALL_STARTS:
                                parameterFunc = "org.docero.data.utils.DDataLike.allStarts(" + parameter.name + ")";
                                break;
                            case LIKE_ALL_HAS:
                                parameterFunc = "org.docero.data.utils.DDataLike.allHas(" + parameter.name + ")";
                                break;
                        }
                    }
                    cf.println("this.put(\"" + parameter.name + "\", " + parameterFunc + ");");
                }
                cf.endBlock("}}");
            }

            if (returnType != null)
                parameters.stream().filter(p -> ROW_BOUNDS_CLASS.equals(p.type.toString())).findAny().ifPresent(p -> {
                    try {
                        cf.println(", " + p.name);
                    } catch (IOException ignore) {
                    }
                });
            cf.println(");");
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
            cf.startBlock("public " + typeVariables2String() + returnType + " get(" +
                    repositoryBuilder.idClass + " id)" + throwsPart + " {");
            cf.println("return get(id, " +
                    DataBeanBuilder.dateNowFrom(repositoryBuilder.versionalType) +
                    ");");
            cf.endBlock("}");
        }
    }

    private String typeVariables2String() {
        if (typeVariables.isEmpty()) return "";
        return "<" + typeVariables.stream()
                .map(tv -> tv.asElement() + " extends " + tv.getUpperBound())
                .collect(Collectors.joining(",")) + "> ";
    }

    void setFiltersAndOrder(List<DDataMapBuilder.FilterOption> filters, VariableElement order) {
        if (filters != null) this.filters = filters;
        this.order = order;
    }

    List<DDataMapBuilder.FilterOption> getFilters() {
        return filters;
    }

    VariableElement getOrder() {
        return order;
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
