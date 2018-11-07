package org.docero.data.processor;

import org.docero.data.*;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.*;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.docero.data.processor.DDataMethodBuilder.MType.*;

class DDataMethodBuilder {
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
    final String updateId;
    final String insertId;
    final String deleteId;
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
            InsertId insert = methodElement.getAnnotation(InsertId.class);
            insertId = insert != null ? insert.value() : null;
            UpdateId update = methodElement.getAnnotation(UpdateId.class);
            updateId = update != null ? update.value() : null;
            DeleteId delete = methodElement.getAnnotation(DeleteId.class);
            deleteId = delete != null ? delete.value() : null;
            selectId = null;
            String nameString = methodName.toLowerCase();

            this.methodType = insertId != null || nameString.contains("insert") ? MType.INSERT : (
                    updateId != null || nameString.contains("update") ? MType.UPDATE :
                            MType.DELETE
            );
            returnSimpleType = false;
        } else {
            SelectId select = methodElement.getAnnotation(SelectId.class);
            selectId = select != null ? select.value() : null;
            updateId = null;
            insertId = null;
            deleteId = null;
            String ttype = returnType.toString();

            if (ttype.contains("java.util.List")) methodType = MType.SELECT;
            else if (ttype.contains("java.util.Map")) methodType = MType.SELECT;
            else methodType = GET;
            returnSimpleType = repositoryBuilder.rootBuilder.isSimpleMappedType(returnType);
        }
        boolean defaultMethod = ("get".equals(methodName) && methodElement.getParameters().size() == 1) || (
                ("update".equals(methodName) || "insert".equals(methodName) || "delete".equals(methodName)
                ) && returnNothing && methodElement.getParameters().size() == 1);
        methodIndex = defaultMethod ? 0 : repositoryBuilder.methods.stream()
                .filter(m -> methodName.equals(m.methodName)).count() + 1;
    }

    DDataMethodBuilder(
            DataRepositoryBuilder repositoryBuilder,
            DataBeanBuilder bean,
            MType methodType
    ) {
        this.repositoryBuilder = repositoryBuilder;
        typeVariables = Collections.emptyList();
        returnSimpleType = false;
        this.methodType = methodType;
        methodIndex = 0;
        throwTypes = Collections.emptyList();
        Elements e = repositoryBuilder.rootBuilder.environment.getElementUtils();
        Types tu = repositoryBuilder.rootBuilder.environment.getTypeUtils();
        switch (methodType) {
            case GET:
                methodName = "get";
                returnType = repositoryBuilder.forInterfaceName;
                if (repositoryBuilder.versionalType == null)
                    parameters.add(new DDataMethodParameter("id", repositoryBuilder.idClass));
                else {
                    parameters.add(new DDataMethodParameter("id",
                            e.getTypeElement(bean.keyType).asType()));
                }
                break;
            case INSERT:
                methodName = "insert";
                returnType = repositoryBuilder.forInterfaceName;
                parameters.add(new DDataMethodParameter("bean", repositoryBuilder.forInterfaceName));
                break;
            case UPDATE:
                methodName = "update";
                returnType = repositoryBuilder.forInterfaceName;
                parameters.add(new DDataMethodParameter("bean", repositoryBuilder.forInterfaceName));
                break;
            case DELETE:
                methodName = "delete";
                returnType = null;
                if (repositoryBuilder.versionalType == null)
                    parameters.add(new DDataMethodParameter("id", repositoryBuilder.idClass));
                else {
                    parameters.add(new DDataMethodParameter("id",
                            e.getTypeElement(bean.keyType).asType()));
                }
                break;
            default:
                methodName = "list";
                returnType = tu.getDeclaredType(
                        e.getTypeElement("java.util.List"), repositoryBuilder.forInterfaceName
                );
        }
        selectId = null;
        insertId = null;
        deleteId = null;
        updateId = null;
    }

    void build(JavaClassWriter cf) throws IOException {
        String throwsPart = throwTypes.size() > 0 ?
                "throws " + throwTypes.stream().map(TypeMirror::toString).collect(Collectors.joining(", ")) :
                "";
        cf.println("");
        DataBeanBuilder bean = repositoryBuilder.rootBuilder.beansByInterface.get(repositoryBuilder.forInterfaceName());

        List<MappedDictionary> dictionariesUpdatedByThis_temp = methodType == GET ? new ArrayList<>() :
                searchForDictionariesUseThisBean(parameters, methodType == UPDATE);

        if (bean.dictionary != DictionaryType.NO && repositoryBuilder.rootBuilder.useSpringCache && methodIndex == 0) {
            switch (methodType) {
                case GET:
                    cf.println("@org.springframework.cache.annotation.Cacheable(cacheNames=\"" + bean.cacheMap +
                            "\", sync=true)");
                    break;
                case DELETE:
                    dictionariesUpdatedByThis_temp.add(new MappedDictionary(bean, parameters.get(0), parameters, false));
                    break;
                case INSERT:
                case UPDATE:
                    if (dictionariesUpdatedByThis_temp.stream().noneMatch(dd -> dd.bean == bean && dd.cacheKey == null))
                        cf.println("@org.springframework.cache.annotation.CachePut(cacheNames=\"" +
                                bean.cacheMap + "\", key = \"#bean.dDataBeanKey_\")");
            }
        }

        List<MappedDictionary> dictionariesUpdatedByThis = dictionariesUpdatedByThis_temp.stream()
                .filter(d ->
                        d.cacheKey == null || dictionariesUpdatedByThis_temp.stream()
                                .noneMatch(dd -> dd.bean == d.bean && dd.cacheKey == null)
                )
                .collect(Collectors.toList());
        if (!dictionariesUpdatedByThis.isEmpty()) {
            boolean multi = dictionariesUpdatedByThis.size() > 1;
            if (multi) cf.startBlock("@org.springframework.cache.annotation.Caching(evict = {");

            for (String s : dictionariesUpdatedByThis.stream().map(d ->
                    d.cacheKey != null ?
                            "@org.springframework.cache.annotation.CacheEvict(cacheNames=\"" + d.bean.cacheMap +
                                    "\", key=\"" + d.cacheKey + "\")" :
                            "@org.springframework.cache.annotation.CacheEvict(cacheNames=\"" + d.bean.cacheMap +
                                    "\", allEntries=true)"
            ).collect(Collectors.joining(",\n")).split("\\n")) {
                cf.println(s);
            }
            if (multi) cf.endBlock("})");
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
        String cacheFunction = null;
        String beanParameterName = parameters.size() == 1 ? parameters.get(0).name : null;
        String selectId = this.selectId != null ? this.selectId : repositoryBuilder.mappingClassName + "." +
                methodName + (methodIndex == 0 ? "" : "_" + methodIndex);
        String insertId = this.insertId != null ? this.insertId : repositoryBuilder.mappingClassName + "." +
                methodName + (methodIndex == 0 ? "" : "_" + methodIndex);
        String updateId = this.updateId != null ? this.updateId : repositoryBuilder.mappingClassName + "." +
                methodName + (methodIndex == 0 ? "" : "_" + methodIndex);
        String deleteId = this.deleteId != null ? this.deleteId : repositoryBuilder.mappingClassName + "." +
                methodName + (methodIndex == 0 ? "" : "_" + methodIndex);
        boolean useParameterBean = false;
        boolean returnSomething = returnType != null && returnType.getKind() != TypeKind.VOID;
        switch (methodType) {
            case GET:
                cf.print("return getSqlSession().selectOne(\"" + selectId + "\"");
                break;
            case SELECT:
                if (returnSomething) {
                    cf.print("return getSqlSession().");
                    if (returnType.toString().contains("java.util.List")) {
                        cf.print("selectList(");
                    } else if (returnType.toString().contains("java.util.Map")) {
                        cf.print("selectMap(");
                    } else {
                        cf.print("selectOne(");
                    }
                }
                cf.print("\"" + selectId + "\"");
                break;
            case INSERT:
                useParameterBean = true;
                if (bean.dictionary == DictionaryType.SMALL && dictionariesUpdatedByThis.stream()
                        .noneMatch(d -> d.bean == bean))
                    cf.println("updateVersion(" + repositoryBuilder.forInterfaceName() + ".class);");
                if (repositoryBuilder.versionalType != null) {
                    cf.println(bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny().map(p ->
                            beanParameterName + ".set" + Character.toUpperCase(p.name.charAt(0)) + p.name.substring(1) +
                                    "(" + DataBeanBuilder.dateNowFrom(bean.versionalType) + ");")
                            .orElse("")
                    );
                }
                if (repositoryBuilder.discriminator != null)
                    for (DataRepositoryDiscriminator.Item item : repositoryBuilder.discriminator.beans) {
                        //DataRepositoryBuilder strep = repositoryBuilder.rootBuilder.repositoriesByBean.get(item.beanInterface);
                        cf.startBlock("if (" + beanParameterName + " instanceof " + item.beanInterface + ") {");
                        cf.println("getSqlSession().insert(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                                methodIndex == 0 ? "" : "_" + methodIndex) + "_" +
                                item.beanInterfaceShort() + "\", " + beanParameterName + ");");
                        if (returnSomething) cf.println("return " + beanParameterName + ";");
                        cf.endBlock("}");
                    }
                cf.print("getSqlSession().insert");
                cf.print("(\"" + insertId + "\"");
                if (bean.dictionary != DictionaryType.NO && !repositoryBuilder.rootBuilder.useSpringCache)
                    cacheFunction = "cache(" + beanParameterName + ");";
                break;
            case UPDATE:
                useParameterBean = true;
                if (bean.dictionary == DictionaryType.SMALL && dictionariesUpdatedByThis.stream()
                        .noneMatch(d -> d.bean == bean))
                    cf.println("updateVersion(" + repositoryBuilder.forInterfaceName() + ".class);");
                if (repositoryBuilder.versionalType != null) {
                    cf.println(bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny().map(p ->
                            beanParameterName + ".set" + Character.toUpperCase(p.name.charAt(0)) + p.name.substring(1) +
                                    "(" + DataBeanBuilder.dateNowFrom(bean.versionalType) + ");")
                            .orElse("")
                    );
                }
                if (repositoryBuilder.discriminator != null)
                    for (DataRepositoryDiscriminator.Item item : repositoryBuilder.discriminator.beans) {
                        //DataRepositoryBuilder strep = repositoryBuilder.rootBuilder.repositoriesByBean.get(item.beanInterface);
                        cf.startBlock("if (" + beanParameterName + " instanceof " + item.beanInterface + ") {");
                        cf.println("getSqlSession().update(\"" + repositoryBuilder.mappingClassName + "." + methodName + (
                                methodIndex == 0 ? "" : "_" + methodIndex) + "_" +
                                item.beanInterfaceShort() + "\", " + beanParameterName + ");");
                        if (returnSomething) cf.println("return " + beanParameterName + ";");
                        cf.endBlock("}");
                    }
                cf.print("getSqlSession().update");
                cf.print("(\"" + updateId + "\"");
                if (bean.dictionary != DictionaryType.NO && !repositoryBuilder.rootBuilder.useSpringCache)
                    cacheFunction = "cache(" + beanParameterName + ");";
                break;
            default: // DELETE
                if (bean.dictionary == DictionaryType.SMALL && dictionariesUpdatedByThis.stream()
                        .noneMatch(d -> d.bean == bean))
                    cf.println("updateVersion(" + repositoryBuilder.forInterfaceName() + ".class);");
                cf.print("getSqlSession().delete(\"" + deleteId + "\"");
        }

        if (parameters.size() > 0) {
            if (parameters.size() == 1 &&
                    (methodType == GET || methodType == DELETE) && methodIndex == 0
                    ) {
                cf.startBlock(", ");
                cf.startBlock("new java.util.HashMap<java.lang.String, java.lang.Object>(){{");
                for (DataBeanPropertyBuilder property : bean.properties.values())
                    if (property.isId) {
                        cf.println("this.put(\"" + property.name + "\", " + parameters.get(0).name +
                                (bean.isKeyComposite ?
                                        ".get" + Character.toUpperCase(property.name.charAt(0)) +
                                                property.name.substring(1) + "()" :
                                        "") + ");");
                    }
                cf.endBlock("}}");
                cf.endBlock();
            } else if (parameters.size() == 1 && useParameterBean &&
                    parameters.get(0).type.equals(repositoryBuilder.forInterfaceName)) {
                cf.print(", " + parameters.get(0).name);
            } else {
                cf.startBlock(", ");
                cf.startBlock("new java.util.HashMap<java.lang.String, java.lang.Object>(){{");
                boolean hasVersionFilter = false;
                for (DDataMethodParameter parameter : parameters) {
                    DDataMapBuilder.FilterOption filter = filters.stream()
                            .filter(f -> f.parameter != null && f.parameter.equals(parameter.name))
                            .findAny().orElse(null);
                    String parameterFunc = parameter.name;
                    if (filter != null) {
                        if ("VERSION_".equals(filter.enumName)) hasVersionFilter = true;

                        switch (filter.option) {
                            case LIKE:
                            case ILIKE:
                                parameterFunc = "org.docero.data.utils.DDataLike.in(" + parameter.name + ")";
                                break;
                            case LIKE_HAS:
                            case ILIKE_HAS:
                            case LIKE_HAS_ALL:
                            case ILIKE_HAS_ALL:
                                parameterFunc = "org.docero.data.utils.DDataLike.has(" + parameter.name + ")";
                                break;
                            case LIKE_ENDS:
                            case ILIKE_ENDS:
                                parameterFunc = "org.docero.data.utils.DDataLike.ends(" + parameter.name + ")";
                                break;
                            case LIKE_STARTS:
                            case ILIKE_STARTS:
                                parameterFunc = "org.docero.data.utils.DDataLike.starts(" + parameter.name + ")";
                                break;
                        }
                    }
                    cf.println("this.put(\"" + parameter.name + "\", " + parameterFunc + ");");
                }

                if (repositoryBuilder.versionalType != null && !hasVersionFilter && methodType == SELECT) {
                    Optional<DataBeanPropertyBuilder> versionProp = bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny();
                    cf.println("this.put(\"" + versionProp.get().name + "\", " +
                            DataBeanBuilder.dateNowFrom(repositoryBuilder.versionalType) +
                            ");");
                }
                cf.endBlock("}}");
                cf.endBlock();
            }
            cf.println(");");
        } else {
            cf.println(");");
        }

        if (cacheFunction != null) cf.println(cacheFunction);

        if (dictionariesUpdatedByThis != null)
            for (MappedDictionary d : dictionariesUpdatedByThis)
                if (d.bean.dictionary == DictionaryType.SMALL) {
                    if (d.cacheKey != null) cf.println("updateVersion(" + d.bean.interfaceType + ".class);");
                    else cf.println("clearVersion(" + d.bean.interfaceType + ".class);");
                }

        if (returnSomething && (methodType == INSERT || methodType == UPDATE))
            cf.println("return " + beanParameterName + ";");

        cf.endBlock("}");

        if (methodIndex == 0) {
            if (repositoryBuilder.versionalType != null)
                if (methodType == GET) {
                    cf.println("");
                    cf.startBlock("public " + typeVariables2String() + returnType + " get(" +
                            repositoryBuilder.idClass + " id)" + throwsPart + " {");
                    cf.println("return get(id, " +
                            DataBeanBuilder.dateNowFrom(repositoryBuilder.versionalType) +
                            ");");
                    cf.endBlock("}");

                    cf.println("");
                    cf.startBlock("public " + typeVariables2String() + returnType + " get(" +
                            repositoryBuilder.idClass + " id, " +
                            repositoryBuilder.versionalType + " at) {");
                    cf.println("return getSqlSession().selectOne(\"" +
                            repositoryBuilder.mappingClassName + ".get\", new " + bean.keyType + "(id, at));");
                    cf.endBlock("}");
                } else if (methodType == DELETE) {
                    cf.println("");
                    cf.startBlock("public void delete(" +
                            repositoryBuilder.idClass + " id) {");
                    cf.println("delete(new " + bean.keyType + "(id, " +
                            DataBeanBuilder.dateNowFrom(repositoryBuilder.versionalType) +
                            "));");
                    cf.endBlock("}");
                }
        }
    }

    private List<MappedDictionary> searchForDictionariesUseThisBean(ArrayList<DDataMethodParameter> parameters, boolean forImmutableOnly) {
        DataBeanBuilder thisBean =
                repositoryBuilder.rootBuilder.beansByInterface.get(repositoryBuilder.forInterfaceName());
        List<MappedDictionary> list = new ArrayList<>();
        this.repositoryBuilder.rootBuilder.mappings.values().forEach(m -> {
            DataBeanBuilder mappedBean = m.mappedProperties.get(0).dataBean;
            DataBeanBuilder mapFromBean = m.properties.get(0).dataBean;
            if (mappedBean == thisBean)
                if ((thisBean.dictionary == DictionaryType.SMALL &&
                        mapFromBean.dictionary == DictionaryType.SMALL && m.manyToOne
                ) || (thisBean.dictionary != DictionaryType.SMALL && mapFromBean.isDictionary())
                        ) list.add(new MappedDictionary(mapFromBean, m, parameters, forImmutableOnly));
        });
        return list;
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

    static class MappedDictionary {
        private final TypeMirror myType;
        private final TypeMirror myKeyType;
        final DataBeanBuilder bean;
        final String cacheKey;
        DataBeanPropertyBuilder property;

        MappedDictionary(DataBeanBuilder bean, Mapping m, ArrayList<DDataMethodParameter> parameters, boolean forImmutableOnly) {
            this.bean = bean;
            DataBeanBuilder myBean = m.mappedProperties.get(0).dataBean;
            this.myType = myBean.interfaceType;
            TypeElement keyTypeElement = myBean.rootBuilder.environment.getElementUtils()
                    .getTypeElement(myBean.keyType);
            this.myKeyType = keyTypeElement.asType();

            List<DataBeanPropertyBuilder> dictionaryIds = bean.properties.values().stream()
                    .filter(DataBeanPropertyBuilder::isId)
                    .collect(Collectors.toList()); // dictionary Properties
            if (dictionaryIds.size() == 1) {
                List<DataBeanPropertyBuilder> mappedIds = m.properties.stream()
                        .filter(DataBeanPropertyBuilder::isId)
                        .collect(Collectors.toList()); // dictionary Properties used in mapping
                if (mappedIds.size() == dictionaryIds.size()) {
                    //all in mappings, we can create id for dictionary entry
                    //TODO can i build id for composite key for CacheEvict?
                    property = m.mappedProperties.get(m.properties.indexOf(mappedIds.get(0)));
                }
            }

            Types tu = bean.rootBuilder.environment.getTypeUtils();
            this.cacheKey = property == null || (forImmutableOnly && !property.isId()) ? null : parameters.stream()
                    .filter(p -> tu.isSubtype(p.type, myType)).findAny()
                    .map(p -> p.type.getKind().isPrimitive() || property.isId() ?
                            "#" + p.name + "." + property.name :
                            "#" + p.name + "." + property.name + "==null?'null':#" + p.name + "." + property.name
                    )
                    .orElse(parameters.stream()
                            .filter(p ->
                                    p.type.toString().equals(myKeyType.toString()) && property.isId()
                            ).findAny()
                            .map(p -> "#" + p.name + "." + property.name)
                            .orElse(null)
                    );
        }

        MappedDictionary(DataBeanBuilder bean, DDataMethodParameter dDataMethodParameter, ArrayList<DDataMethodParameter> parameters, boolean forImmutableOnly) {
            this.bean = bean;
            this.myType = null;
            this.myKeyType = null;
            this.cacheKey = dDataMethodParameter.type.getKind().isPrimitive() ?
                    "#" + dDataMethodParameter.name :
                    "#" + dDataMethodParameter.name + "==null?'null':#" + dDataMethodParameter.name;
        }
    }
}
