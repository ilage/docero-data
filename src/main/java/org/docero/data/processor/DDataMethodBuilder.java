package org.docero.data.processor;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DDataMethodBuilder {
    final TypeMirror returnType;
    final String methodName;
    final ArrayList<DDataMethodParameter> parameters = new ArrayList<>();
    final List<? extends TypeMirror> throwTypes;
    final DataRepositoryBuilder repositoryBuilder;
    final long methodIndex;

    DDataMethodBuilder(DataRepositoryBuilder repositoryBuilder, ExecutableElement methodElement, ProcessingEnvironment environment) {
        this.repositoryBuilder = repositoryBuilder;
        returnType = methodElement.getReturnType();
        methodName = methodElement.getSimpleName().toString();
        methodIndex = repositoryBuilder.methods.stream().filter(m -> methodName.equals(m.methodName)).count();
        for (VariableElement variableElement : methodElement.getParameters()) {
            parameters.add(new DDataMethodParameter(variableElement.getSimpleName(), variableElement.asType()));
        }
        throwTypes = methodElement.getThrownTypes();
    }

    void build(JavaClassWriter cf, Map<String, DataBeanBuilder> beansByInterface) throws IOException {
        cf.println("");
        cf.startBlock("public " + (returnType == null ? "void" : returnType) + " " + methodName + "(");
        int i = 0;
        for (DDataMethodParameter parameter : parameters) {
            cf.print("" + parameter.type + " " + parameter.name);
            if (++i < parameters.size()) cf.println(",");
            else cf.println("");
        }
        cf.endBlock(")");
        if (throwTypes.size() != 0) {
            cf.print("throws ");
            i = 0;
            for (TypeMirror throwType : throwTypes) {
                cf.print(throwType.toString());
                if (++i < throwTypes.size()) cf.print(", ");
                else cf.print(" ");
            }
        }
        cf.startBlock("{");
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
            //TODO if(methodType)
            if (methodName.contains("insert")) cf.print("insert");
            else if (methodName.contains("update")) cf.print("update");
            else cf.print("delete");
        }
        cf.print("(\"" + repositoryBuilder.mappingClassName + "." + methodName + "_" + methodIndex + "\"");

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
    }

    static class DDataMethodParameter {
        final String name;
        final TypeMirror type;

        DDataMethodParameter(Name simpleName, TypeMirror typeMirror) {
            this.name = simpleName.toString();
            this.type = typeMirror;
        }
    }
}
