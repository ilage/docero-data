package org.docero.data.processor;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
    }

    void build(JavaClassWriter cf) throws IOException {
        String throwsPart = throwTypes.size() > 0 ?
                "throws " + throwTypes.stream().map(TypeMirror::toString).collect(Collectors.joining(", ")) :
                "";
        cf.println("");
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
        cf.print("(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                repositoryBuilder.defaultGetMethod == this ? "" : "_" + methodIndex) + "\"");

        if (parameters.size() > 0) {
            if (parameters.size() == 1 && parameters.get(0).type.equals(repositoryBuilder.forInterfaceName)) {
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
