package org.docero.data.processor;

import org.docero.data.DDataFetchType;
import org.docero.data.DDataFilterOption;
import org.docero.data.DictionaryType;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class DDataMapBuilder {
    private final DDataBuilder builder;
    private final ProcessingEnvironment environment;
    private final HashSet<String> userMappingFiles = new HashSet<>();

    DDataMapBuilder(DDataBuilder builder, ProcessingEnvironment environment) {
        this.builder = builder;
        this.environment = environment;
    }

    boolean build(HashMap<String, TypeElement> pkgClasses) throws Exception {
        if (builder.beansByInterface.isEmpty()) return false;

        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setIgnoringComments(false);
        docBuilderFactory.setNamespaceAware(true);
        docBuilderFactory.setValidating(false);

        for (DataRepositoryBuilder repository : builder.repositories) {
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document mapperDoc = docBuilder.newDocument();
            org.w3c.dom.Element mapperRoot = (org.w3c.dom.Element)
                    mapperDoc.appendChild(mapperDoc.createElement("mapper"));

            TypeElement repositoryElement = pkgClasses.get(repository.repositoryInterface.toString());
            String repositoryNamespace;
            FetchOptions defaultFetchOptions = new FetchOptions(1);
            DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
            if (repositoryElement == null) {
                // методов с фильтрами нет, можно строить репозиторий, для мультиклассовых
                repository.build(environment, builder.spring);
                repositoryNamespace = repository.forInterfaceName();
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.GET),
                        defaultFetchOptions, repository);
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.INSERT),
                        defaultFetchOptions, repository);
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.UPDATE),
                        defaultFetchOptions, repository);
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.DELETE),
                        defaultFetchOptions, repository);
                if (bean.isDictionary())
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.SELECT),
                            defaultFetchOptions, repository);
            } else {
                repositoryNamespace = repository.repositoryInterface.toString();
                //System.out.println(repository.repositoryInterface + ":" + repositoryElement.getEnclosedElements());
                for (Element methodElement : repositoryElement.getEnclosedElements())
                    if (methodElement.getKind() == ElementKind.METHOD && !methodElement.getModifiers().contains(Modifier.STATIC) ?
                            !methodElement.getModifiers().contains(Modifier.DEFAULT) :
                            methodElement.getModifiers().contains(Modifier.ABSTRACT))
                        checkMethodFilters((ExecutableElement) methodElement, repository);
                // методы с фильтрами обновлены, можно строить репозиторий, для мультиклассовых
                repository.build(environment, builder.spring);

                for (Element methodElement : repositoryElement.getEnclosedElements())
                    if (methodElement.getKind() == ElementKind.METHOD && !methodElement.getModifiers().contains(Modifier.STATIC) ?
                            !methodElement.getModifiers().contains(Modifier.DEFAULT) :
                            methodElement.getModifiers().contains(Modifier.ABSTRACT))
                        createDefinedMethod(mapperRoot, (ExecutableElement) methodElement, repository);

                if (repository.defaultGetMethod == null)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.GET),
                            defaultFetchOptions, repository);
                if (!repository.hasInsert)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.INSERT),
                            defaultFetchOptions, repository);
                if (!repository.hasUpdate)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.UPDATE),
                            defaultFetchOptions, repository);
                if (repository.defaultDeleteMethod == null)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.DELETE),
                            defaultFetchOptions, repository);
                if (bean.isDictionary() && repository.defaultListMethod == null)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.SELECT),
                            defaultFetchOptions, repository);
            }
            repository.lazyLoads.values().forEach(mapperRoot::appendChild);
            mapperRoot.setAttribute("namespace", repositoryNamespace);

            repository.setBeansDiscriminatorProperties();

            int nameDi = repositoryNamespace.lastIndexOf('.');
            FileObject mappingResource = environment.getFiler()//.createSourceFile(repository.forInterfaceName+".xml");
                    .createResource(StandardLocation.SOURCE_OUTPUT,
                            repositoryNamespace.substring(0, nameDi),
                            repositoryNamespace.substring(nameDi + 1) + ".xml");
            DocumentType doctype = mapperDoc.getImplementation().createDocumentType("DOCTYPE",
                    "-//mybatis.org//DTD Mapper 3.0//EN", "http://mybatis.org/dtd/mybatis-3-mapper.dtd");
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, doctype.getPublicId());
            transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, doctype.getSystemId());
            DOMSource source = new DOMSource(mapperDoc);
            StreamResult result = new StreamResult(mappingResource.openOutputStream());
            transformer.transform(source, result);
        }

        if (builder.spring) {
            try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.DDataConfiguration")) {
                cf.println("package org.docero.data;");
                for (String pkg : builder.packages) cf.println("import " + pkg + ".*;");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.println("@org.springframework.context.annotation.Configuration");
                cf.startBlock("public class DDataConfiguration {");

                for (DataRepositoryBuilder repository : builder.repositories) {
                    cf.println("@org.springframework.context.annotation.Bean");
                    cf.startBlock("public " + repository.daoClassName + " " + repository.repositoryVariableName +
                            "(org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory" +
                            ") {");
                    DeclaredType getType = environment.getTypeUtils().getDeclaredType(
                            environment.getElementUtils().getTypeElement("org.docero.data.DDataRepository"),
                            repository.forInterfaceName, repository.idClass);

                    cf.println(getType + " r = DData.getRepository(" + repository.forInterfaceName + ".class);");
                    cf.startBlock("if (r != null) {");
                    cf.println(
                            "((org.mybatis.spring.support.SqlSessionDaoSupport) r).setSqlSessionFactory(sqlSessionFactory);");
                    cf.endBlock("}");

                    cf.println("return (" + repository.daoClassName + ") r;");
                    cf.endBlock("}");
                }

                for (BatchRepositoryBuilder repository : builder.batchRepositories) {
                    repository.createSpringBean(cf);
                }
                cf.println("");
                cf.println("@org.springframework.context.annotation.Bean");
                cf.startBlock("public org.docero.data.DDataResources dDataResources(org.springframework.context.ApplicationContext context) {");
                cf.println("org.docero.data.DDataResources r = new org.docero.data.DDataResources();");
                for (DataRepositoryBuilder repository : builder.repositories)
                    cf.println("r.add(context.getResource(\"classpath:" +
                            repository.mappingClassName.replaceAll("\\.", "/") +
                            ".xml\"));");
                for (String umf : userMappingFiles) {
                    cf.println("r.add(context.getResource(\"classpath:" +
                            umf.replaceAll("\\.", "/") +
                            ".xml\"));");
                }
                cf.println("return r;");
                cf.endBlock("}");

                cf.println("");
                cf.println("@org.springframework.context.annotation.Bean");
                cf.startBlock("public org.docero.data.DData dData(");
                for (DataBeanBuilder bean : builder.beansByInterface.values())
                    if (bean.dictionary != DictionaryType.NO) {
                        DataRepositoryBuilder r = builder.repositoriesByBean.get(bean.interfaceType.toString());
                        cf.println(r.repositoryInterface + " " + r.repositoryVariableName + ",");
                    }
                cf.println("org.springframework.context.ApplicationContext context");
                cf.endBlock(")");
                cf.startBlock("{");
                for (DataBeanBuilder bean : builder.beansByInterface.values())
                    if (bean.dictionary != DictionaryType.NO) {
                        DataRepositoryBuilder r = builder.repositoriesByBean.get(bean.interfaceType.toString());
                        cf.println("org.docero.data.DData.registerAsDictionary(" +
                                r.repositoryVariableName + ", " +
                                bean.interfaceType + ".class, " +
                                bean.getImplementationName() + ".class);");
                    }
                cf.println("return new org.docero.data.DData();");
                cf.endBlock("}");

                cf.endBlock("}");
            }
        }

        return true;
    }

    private DDataMethodBuilder findMethodBuilder(ExecutableElement methodElement, DataRepositoryBuilder repository) {
        TypeMirror returnType = methodElement.getReturnType();
        String paramHash = methodElement.getParameters().stream()
                .map(e -> environment.getTypeUtils().erasure(e.asType()))
                .map(TypeMirror::toString)
                .collect(Collectors.joining(","));
        return repository.methods.stream().filter(m ->
                m.methodName.equals(methodElement.getSimpleName().toString()) &&
                        ((
                                (m.returnType == null || m.returnType.getKind() == TypeKind.VOID) &&
                                        (returnType == null || returnType.getKind() == TypeKind.VOID)
                        ) || (
                                m.returnType != null && returnType != null &&
                                        m.returnType.toString().equals(returnType.toString())
                        )) &&
                        m.parameters.size() == methodElement.getParameters().size() &&
                        m.parameters.stream().map(p -> environment.getTypeUtils().erasure(p.type))
                                .map(TypeMirror::toString)
                                .collect(Collectors.joining(","))
                                .equals(paramHash)
        ).findAny().orElse(null);
    }

    private void checkMethodFilters(ExecutableElement methodElement, DataRepositoryBuilder repository) throws Exception {
        DDataMethodBuilder method = findMethodBuilder(methodElement, repository);
        if (method == null)
            throw new Exception("not found info about method '" + methodElement.getSimpleName() +
                    " of " + repository.repositoryInterface);

        if (method.selectId == null) {
            ArrayList<FilterOption> filters = new ArrayList<>();
            VariableElement order = null;
            for (VariableElement variableElement : methodElement.getParameters()) {
                FilterOption filter = new FilterOption(repository, variableElement);
                if (filter.option != null) filters.add(filter);
                else if (variableElement.asType().toString().startsWith("org.docero.data.DDataOrder")) {
                    order = variableElement;
                }
            }
            VariableElement finalOrder = order;
            repository.onMethod(methodElement, m -> m.setFiltersAndOrder(filters, finalOrder));
        }
    }

    private void createDefinedMethod(
            org.w3c.dom.Element mapperRoot,
            ExecutableElement methodElement,
            DataRepositoryBuilder repository
    ) throws Exception {
        DDataMethodBuilder method = findMethodBuilder(methodElement, repository);
        if (method == null)
            throw new Exception("not found info about method '" + methodElement.getSimpleName() +
                    " of " + repository.repositoryInterface);

        FetchOptions fetchOptions = methodElement.getAnnotationMirrors().stream()
                .filter(a -> a.toString().indexOf("_DDataFetch_") > 0)
                .findAny().map(f -> new FetchOptions(repository, f))
                .orElse(new FetchOptions(1));

        createDefinedMethod(mapperRoot, method, fetchOptions, repository);
    }

    private void createDefinedMethod(
            org.w3c.dom.Element mapperRoot,
            DDataMethodBuilder method, FetchOptions fetchOptions,
            DataRepositoryBuilder repository
    ) throws Exception {
        if (method.selectId != null)
            userMappingFiles.add(method.selectId.substring(0, method.selectId.lastIndexOf('.')));

        List<FilterOption> filters = method.getFilters();
        VariableElement order = method.getOrder();

        Document doc = mapperRoot.getOwnerDocument();
        DataBeanBuilder bean = builder.beansByInterface.get(method.repositoryBuilder.forInterfaceName());

        AtomicInteger index = new AtomicInteger();
        CopyOnWriteArrayList<MappedTable> mappedTables = new CopyOnWriteArrayList<>(bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                    return b == null ? null :
                            new MappedTable(0, index.incrementAndGet(), p, b, fetchOptions, filters);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        if (repository.discriminator != null)
            addTables4MultiType(repository.discriminator, mappedTables, index, fetchOptions);

        String methodName = method == repository.defaultGetMethod || method == repository.defaultDeleteMethod ?
                method.methodName :
                method.methodName + (method.methodIndex == 0 ? "" : "_" + method.methodIndex);
        switch (method.methodType) {
            case SELECT:
                if (method.methodIndex == 0) createDictionaryList(mapperRoot, fetchOptions);
            case GET:
                if (fetchOptions.resultMap.length() == 0 && !method.returnSimpleType)
                    buildResultMap(mapperRoot, repository, methodName, fetchOptions, mappedTables, filters);

                org.w3c.dom.Element select = (org.w3c.dom.Element)
                        mapperRoot.appendChild(doc.createElement("select"));
                select.setAttribute("id", methodName);
                select.setAttribute("parameterType", "HashMap");
                if (method.returnType != null && method.returnSimpleType)
                    select.setAttribute("resultType", method.returnType.toString());
                else if (fetchOptions.resultMap.length() == 0)
                    select.setAttribute("resultMap", methodName + "_ResultMap");
                else {
                    select.setAttribute("resultMap", fetchOptions.resultMap);
                    userMappingFiles.add(fetchOptions.resultMap.substring(0, fetchOptions.resultMap.lastIndexOf('.')));
                }
                buildSql(repository, method, bean, fetchOptions, mappedTables, select, filters, order);
                break;
            case INSERT:
                org.w3c.dom.Element insert = (org.w3c.dom.Element)
                        mapperRoot.appendChild(doc.createElement("insert"));
                insert.setAttribute("id", methodName);
                insert.setAttribute("parameterType", bean.getImplementationName());
                buildSql(repository, method, bean, fetchOptions, mappedTables, insert, filters, order);
                break;
            case UPDATE:
                org.w3c.dom.Element update = (org.w3c.dom.Element)
                        mapperRoot.appendChild(doc.createElement("update"));
                update.setAttribute("id", methodName);
                update.setAttribute("parameterType", bean.getImplementationName());
                buildSql(repository, method, bean, fetchOptions, mappedTables, update, filters, order);
                break;
            case DELETE:
                org.w3c.dom.Element delete = (org.w3c.dom.Element)
                        mapperRoot.appendChild(doc.createElement("delete"));
                delete.setAttribute("id", methodName);
                delete.setAttribute("parameterType", "HashMap");
                buildSql(repository, method, bean, fetchOptions, mappedTables, delete, filters, order);
                break;
            default:
                environment.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "Method of unknown type " + methodName + " from " + repository.mappingClassName);
        }
    }

    private void addTables4MultiType(
            DataRepositoryDiscriminator discriminator,
            List<MappedTable> mappedTables,
            AtomicInteger index,
            FetchOptions fetchOptions
    ) {
        for (DataRepositoryDiscriminator.Item item : discriminator.beans) {
            List<MappedTable> beanTables = builder.beansByInterface.get(item.beanInterface).properties.values().stream()
                    .filter(DataBeanPropertyBuilder::notIgnored)
                    .filter(p -> mappedTables.stream().noneMatch(m -> m.property.columnName.equals(p.columnName)))
                    .map(p -> {
                        DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                        return b == null ? null : new MappedTable(0, index.incrementAndGet(), p, b, fetchOptions, null);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            mappedTables.addAll(beanTables);
        }
    }

    private void buildSql(
            DataRepositoryBuilder repository,
            DDataMethodBuilder method,
            DataBeanBuilder bean,
            FetchOptions fetchOptions,
            List<MappedTable> mappedTables,
            org.w3c.dom.Element domElement,
            List<FilterOption> filters,
            VariableElement order
    ) {
        Document doc = domElement.getOwnerDocument();

        StringBuilder sql = new StringBuilder();
        if (fetchOptions.sqlSelect.length() > 0) {
            sql.append("\n");
            String sqlSelect = fetchOptions.sqlSelect;
            for (DDataMethodBuilder.DDataMethodParameter parameter : method.parameters) {
                TypeMirror pType = parameter.type.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) parameter.type).asType() :
                        parameter.type;
                sqlSelect = sqlSelect.replaceAll(":" + parameter.name,
                        "#{" + parameter.name + ", javaType=" + pType +
                                jdbcTypeParameterFor(pType) +
                                "}");
            }
            sql.append(sqlSelect).append("\n");
            domElement.appendChild(doc.createTextNode(sql.toString()));

        } else {
            boolean limitedSelect = false;
            switch (method.methodType) {
                case SELECT:
                    limitedSelect = filters.stream().anyMatch(f -> f.option == DDataFilterOption.LIMIT);
                case GET:
                    sql.append("\nSELECT\n");
                    if (bean.versionalType != null) {
                        bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny().ifPresent(p -> {
                            sql.append("  ");
                            if (environment.getTypeUtils().directSupertypes(p.type).stream()
                                    .anyMatch(c -> c.toString().equals(builder.temporalType.toString())) ||
                                    environment.getTypeUtils().isSubtype(p.type, builder.oldDateType))
                                sql.append("CAST(").append(buildSqlParameter(bean, p)).append(" AS TIMESTAMP)");
                            else
                                sql.append(buildSqlParameter(bean, p));
                            sql.append(" AS dDataBeanActualAt_,\n");
                        });
                    }
                    HashMap<String, DataBeanPropertyBuilder> allProperties = new HashMap<>();
                    allProperties.putAll(bean.properties);
                    if (repository.discriminator != null) {
                        for (DataRepositoryDiscriminator.Item item : repository.discriminator.beans) {
                            DataBeanBuilder subBean = builder.beansByInterface.get(item.beanInterface);
                            subBean.properties.forEach((name, builder) -> {
                                if (!allProperties.containsKey(name))
                                    allProperties.put(name, builder);
                            });
                        }
                    }
                    sql.append(allProperties.values().stream()
                            .filter(DataBeanPropertyBuilder::notIgnored)
                            .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                            .filter(this::notManagedBean)
                            .filter(fetchOptions::filterIgnored)
                            .map(p -> "  " + p.getColumnReader(0) + " AS " + p.getColumnRef())
                            .collect(Collectors.joining(",\n")));
                    if (fetchOptions.fetchType != DDataFetchType.NO)
                        mappedTables.stream()
                                .filter(MappedTable::notSingleSmallDictionaryValue)
                                .filter(MappedTable::useInFieldsList)
                                .forEach(t -> addManagedBeanToFrom(sql, t, fetchOptions));
                    if (!limitedSelect) sql.append("\nFROM ").append(bean.getTableRef()).append(" AS t0\n");
                    break;
                case INSERT:
                    generateValuesForBean(bean, domElement);
                    generateInsertForBean(sql, repository, bean, fetchOptions);
                    if (repository.discriminator != null) {
                        for (DataRepositoryDiscriminator.Item item : repository.discriminator.beans) {
                            DataBeanBuilder itemBean = repository.rootBuilder.beansByInterface.get(item.beanInterface);
                            org.w3c.dom.Element beanInsert = (org.w3c.dom.Element)
                                    domElement.getParentNode().appendChild(doc.createElement("insert"));
                            beanInsert.setAttribute("id", method.methodName +
                                    (method.methodIndex == 0 ? "" : "_" + method.methodIndex) +
                                    "_" + item.beanInterfaceShort());
                            beanInsert.setAttribute("parameterType", itemBean.getImplementationName());
                            StringBuilder beanSql = new StringBuilder();
                            generateValuesForBean(itemBean, beanInsert);
                            generateInsertForBean(beanSql, repository, itemBean, fetchOptions);
                            beanInsert.appendChild(doc.createTextNode(beanSql.toString()));
                        }
                    }
                    break;
                case UPDATE:
                    generateUpdateForBean(sql, repository, bean, fetchOptions);
                    if (repository.discriminator != null) {
                        for (DataRepositoryDiscriminator.Item item : repository.discriminator.beans) {
                            DataBeanBuilder itemBean =
                                    repository.rootBuilder.beansByInterface.get(item.beanInterface);
                            /*DataRepositoryBuilder itemRepository =
                                    repository.rootBuilder.repositoriesByBean.get(itemBean.interfaceType.toString());*/
                            org.w3c.dom.Element beanUpdate = (org.w3c.dom.Element)
                                    domElement.getParentNode().appendChild(doc.createElement("update"));
                            beanUpdate.setAttribute("id", method.methodName +
                                    (method.methodIndex == 0 ? "" : "_" + method.methodIndex) +
                                    "_" + item.beanInterfaceShort());
                            beanUpdate.setAttribute("parameterType", itemBean.getImplementationName());
                            StringBuilder beanSql = new StringBuilder();
                            generateUpdateForBean(beanSql, repository, itemBean, fetchOptions);
                            if (itemBean.versionalType == null)
                                buildWhereByIds(beanSql, itemBean, method);
                            beanUpdate.appendChild(doc.createTextNode(beanSql.toString()));
                        }
                    }
                    break;
                case DELETE:
                    sql.append("\nDELETE FROM ").append(bean.getTableRef()).append(" AS t0\n");
            }

            switch (method.methodType) {
                case INSERT:
                    domElement.appendChild(doc.createTextNode(sql.toString()));
                    break;
                case UPDATE:
                case GET:
                case DELETE:
                    if (filters != null && filters.size() > 0) {
                        if (method.methodType == DDataMethodBuilder.MType.GET && method.methodIndex == 0) {
                            buildSelect4DefaultGet(domElement, sql);
                            domElement.appendChild(doc.createTextNode("\nWHERE " +
                                    filters.stream()
                                            .filter(f -> f.property != null)
                                            .map(f -> (f.property != null ? f.property.getColumnRef() : null) + " = " +
                                                    buildSqlParameter(bean, f.property))
                                            .collect(Collectors.joining(" AND ")) +
                                    "\n"));
                        } else
                            addFiltersToSql(domElement, sql, bean, method, mappedTables, filters, order, true);
                    } else {
                        if (method.methodType == DDataMethodBuilder.MType.GET)
                            addJoins(mappedTables, sql);

                        StringBuilder ssql;
                        if (method.methodType == DDataMethodBuilder.MType.GET && method.methodIndex == 0) {
                            buildSelect4DefaultGet(domElement, sql);
                            ssql = new StringBuilder("\n");
                        } else ssql = sql;

                        if (method.methodType != DDataMethodBuilder.MType.UPDATE || bean.versionalType == null)
                            buildWhereByIds(ssql, bean, method);

                        domElement.appendChild(doc.createTextNode(ssql.toString()));
                    }
                    break;
                default: //LIST
                    if (limitedSelect) {
                        sql.append("\nFROM (SELECT * FROM ").append(bean.getTableRef()).append(" AS t0\n");
                        domElement.appendChild(doc.createTextNode(sql.toString()));

                        StringBuilder ssql = new StringBuilder();
                        addFiltersToSql(domElement, ssql, bean, method, mappedTables, filters, order, false);
                        ssql.append("\n) AS t0\n");
                        addJoins(mappedTables, ssql);
                        domElement.appendChild(doc.createTextNode(ssql.toString()));
                    } else if (filters != null && filters.size() > 0) {
                        addFiltersToSql(domElement, sql, bean, method, mappedTables, filters, order, true);
                    } else {
                        addJoins(mappedTables, sql);
                        domElement.appendChild(doc.createTextNode(sql.toString()));
                        if (order != null)
                            addOrder(order, mappedTables.stream()
                                    .filter(MappedTable::useInFieldsListOrFilters)
                                    .collect(Collectors.toList()), domElement);
                        else if (!fetchOptions.order.isEmpty())
                            domElement.appendChild(doc.createTextNode("\nORDER BY " +
                                    fetchOptions.order.stream()
                                            .map(o -> o.getColumnRef() + " ASC")
                                            .collect(Collectors.joining(", "))));
                    }
            }
        }
    }

    private void buildWhereByIds(
            StringBuilder ssql, DataBeanBuilder bean, DDataMethodBuilder method
    ) {
        ssql.append("WHERE ");
        if (bean.versionalType == null) {
            ssql.append(bean.properties.values().stream()
                    .filter(DataBeanPropertyBuilder::notIgnored)
                    .filter(p -> p.isId)
                    .map(p -> "t0." + p.getColumnRef() + " = " + buildSqlParameter(bean, p))
                    .collect(Collectors.joining(" AND ")))
                    .append("\n");
        } else {
            ssql.append(bean.properties.values().stream()
                    .filter(p -> p.isId && !p.isVersionFrom)
                    .map(p -> "t0." + p.getColumnRef() + " = " + buildSqlParameter(bean, p))
                    .collect(Collectors.joining(" AND ")))
                    .append("\n");
            if (method.methodType == DDataMethodBuilder.MType.GET) {
                ssql.append(bean.properties.values().stream()
                        .filter(p -> p.isVersionFrom)
                        .findAny().map(p -> " AND t0." + p.getColumnRef() +
                                " <= " + buildSqlParameter(bean, p) +
                                bean.properties.values().stream()
                                        .filter(p1 -> p1.isVersionTo)
                                        .findAny().map(p1 -> "\n AND (t0." + p1.getColumnRef() +
                                        " > " + buildSqlParameter(bean, p) + " OR t0." + p1.getColumnRef() +
                                        " IS NULL)")
                                        .orElse("")
                        )
                        .orElse(""));
            } else if (method.methodType == DDataMethodBuilder.MType.UPDATE) {
                ssql.append(bean.properties.values().stream()
                        .filter(p -> p.isVersionFrom)
                        .findAny().map(p -> " AND t0." + p.getColumnRef() +
                                " = " + buildSqlParameter(bean, p))
                        .orElse(""));
            }
        }
    }

    private void generateUpdateForBean(
            StringBuilder sql, DataRepositoryBuilder repository, DataBeanBuilder bean, FetchOptions fetchOptions
    ) {
        if (bean.versionalType != null) {
            sql.append(bean.properties.values().stream()
                    .filter(p -> p.isVersionFrom).findAny()
                    .map(vfrom -> bean.properties.values().stream()
                            .filter(p -> p.isVersionTo).findAny()
                            .map(vto -> "\nUPDATE " + bean.getTableRef() + " SET " + vto.getColumnRef() +
                                    " = " + buildSqlParameter(bean, vfrom) +
                                    "\nWHERE " +
                                    bean.properties.values().stream()
                                            .filter(p -> p.isId && !p.isVersionFrom)
                                            .map(p -> p.getColumnRef() + " = " + buildSqlParameter(bean, p))
                                            .collect(Collectors.joining(" AND ")) +
                                    " AND " + vfrom.getColumnRef() +
                                    " <= " + buildSqlParameter(bean, vfrom) +
                                    " AND ( " + vto.getColumnRef() +
                                    " > " + buildSqlParameter(bean, vfrom) +
                                    " OR " + vto.getColumnRef() +
                                    " IS NULL);")
                            .orElse("")
                    ).orElse(""));
            generateInsertForBean(sql, repository, bean, fetchOptions);
        } else {
            DataBeanPropertyBuilder dp = repository.discriminator == null ? null : repository.discriminator.property;
            sql.append("\nUPDATE ").append(bean.getTableRef()).append(" AS t0 SET\n");
            sql.append(bean.properties.values().stream()
                    .filter(DataBeanPropertyBuilder::notIgnored)
                    .filter(DataBeanPropertyBuilder::notId)
                    .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                    .filter(p -> filterIgnored(fetchOptions, p))
                    .filter(p -> dp == null || !dp.columnName.equals(p.columnName))
                    .filter(this::notManagedBean)
                    .map(p -> p.getColumnRef() + " = " + buildSqlParameter(bean, p))
                    .collect(Collectors.joining(",\n")))
                    .append("\n");
        }
    }

    private void generateValuesForBean(DataBeanBuilder bean, org.w3c.dom.Element domElement) {
        Document doc = domElement.getOwnerDocument();
        bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::isGenerated)
                .forEach(prop ->
                {
                    org.w3c.dom.Element sk = (org.w3c.dom.Element)
                            domElement.appendChild(doc.createElement("selectKey"));
                    sk.setAttribute("keyProperty", prop.columnName);
                    sk.setAttribute("resultType", prop.type.toString());
                    sk.setAttribute("statementType", "PREPARED");
                    if (prop.generatedStrategy != null) {
                        switch (prop.generatedStrategy) {
                            case SEQUENCE:
                                sk.setAttribute("order", "BEFORE");
                                sk.appendChild(doc.createTextNode("SELECT nextval('" +
                                        prop.generatedValue + "');"));
                                break;
                            case SELECT:
                                sk.setAttribute("order", prop.generatedBefore ? "BEFORE" : "AFTER");
                                sk.appendChild(doc.createTextNode(prop.generatedValue));
                                break;
                            default:
                        }
                    }
                });
    }

    private void generateInsertForBean(
            StringBuilder sql, DataRepositoryBuilder repository, DataBeanBuilder bean, FetchOptions fetchOptions
    ) {
        DataBeanPropertyBuilder dp = repository.discriminator == null ? null : repository.discriminator.property;
        sql.append("\nINSERT INTO ").append(bean.getTableRef()).append(" (");
        sql.append(bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                .filter(this::notManagedBean)
                .filter(p -> filterIgnored(fetchOptions, p))
                .filter(p -> dp == null || !dp.columnName.equals(p.columnName))
                .map(DataBeanPropertyBuilder::getColumnRef)
                .collect(Collectors.joining(", ")));
        if (dp != null) sql.append(", ").append(dp.getColumnRef());
        sql.append(")\n");
        sql.append("VALUES (\n");
        sql.append(bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                .filter(this::notManagedBean)
                .filter(p -> filterIgnored(fetchOptions, p))
                .filter(p -> dp == null || !dp.columnName.equals(p.columnName))
                .map(p -> buildSqlParameter(bean, p))
                .collect(Collectors.joining(",\n")));
        if (dp != null) {
            Optional<DataRepositoryDiscriminator.Item> beanItem = Arrays.stream(repository.discriminator.beans)
                    .filter(i -> i.beanInterface.equals(bean.interfaceType.toString()))
                    .findAny();
            if (beanItem.isPresent()) {
                sql.append(",\n");
                if ("java.lang.String".equals(dp.type.toString()))
                    sql.append('\'').append(beanItem.get().value).append('\'');
                else
                    sql.append(beanItem.get().value);
            }
        }
        sql.append("\n)\n");
    }

    private boolean filterIgnored(FetchOptions fetchOptions, DataBeanPropertyBuilder p) {
        return fetchOptions.ignore.stream().noneMatch(f -> f.name.equals(p.name));
    }

    private boolean notManagedBean(DataBeanPropertyBuilder propertyBuilder) {
        return !this.builder.beansByInterface.containsKey(propertyBuilder.type.toString());
    }

    private void buildSelect4DefaultGet(
            org.w3c.dom.Element domElement,
            StringBuilder sql
    ) {
        Document doc = domElement.getOwnerDocument();
        org.w3c.dom.Element sqle = (org.w3c.dom.Element)
                domElement.getParentNode().appendChild(doc.createElement("sql"));
        sqle.setAttribute("id", "get_select");
        sqle.appendChild(doc.createTextNode(sql.toString()));
        org.w3c.dom.Element sqli = (org.w3c.dom.Element)
                domElement.appendChild(doc.createElement("include"));
        sqli.setAttribute("refid", "get_select");
    }

    private void addFiltersToSql(
            org.w3c.dom.Element domElement,
            StringBuilder sql,
            DataBeanBuilder bean,
            DDataMethodBuilder method,
            List<MappedTable> mappedTables,
            List<FilterOption> filters,
            VariableElement order,
            boolean addJoins
    ) {
        Document doc = domElement.getOwnerDocument();

        List<org.w3c.dom.Element> where = new ArrayList<>();

        //HashSet<MappedTable> joins = new HashSet<>();
        //joins.addAll(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()));

        HashMap<String, IfExists> whereExists = new HashMap<>();
        org.w3c.dom.Element e;
        for (FilterOption filter : filters)
            if (filter.option != null && filter.property != null && filter.mappedBy != null) {
                Optional<MappedTable> table = mappedTables.stream()
                        .filter(mb -> mb.mappedFromTableIndex <= 1 || filter.property.dataBean != bean)
                        .filter(mb -> mb.property.dataBean == filter.mappedBy.dataBean)
                        //.filter(mb -> mb.mappedByProperty.equals(filter.mappedBy.name))
                        .filter(mb -> mb.property.name.equals(filter.mappedBy.name)
                        ).findAny();
                //table.ifPresent(joins::add);
                int tIdx = table.map(mb -> mb.tableIndex).orElse(0);
                MappedTable currentJoin = table.orElse(null);

                String filterParameter = filter.property.getColumnWriter("#{" + filter.parameter + ", javaType=" + (filter.variableType.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) filter.variableType) :
                        filter.variableType
                ) + jdbcTypeParameterFor(filter.variableType) + "}");

                switch (filter.option) {
                    case EQUALS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " = " + filterParameter + "\n"));
                        break;
                    case NOT_EQUALS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " != " + filterParameter + "\n"));
                        break;
                    case LOWER_THAN:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " < " + filterParameter + "\n"));
                        break;
                    case NO_LOWER_THAN:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " >= " + filterParameter + "\n"));
                        break;
                    case GREATER_THAN:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " > " + filterParameter + "\n"));
                        break;
                    case NO_GREATER_THAN:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " <= " + filterParameter + "\n"));
                        break;
                    case IS_NULL:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");

                        org.w3c.dom.Element e_c = (org.w3c.dom.Element)
                                e.appendChild(doc.createElement("choose"));

                        org.w3c.dom.Element e_w = (org.w3c.dom.Element)
                                e_c.appendChild(doc.createElement("when"));
                        e_w.setAttribute("test", filter.parameter);
                        e_w.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " IS NULL\n"));

                        org.w3c.dom.Element e_o = (org.w3c.dom.Element)
                                e_c.appendChild(doc.createElement("otherwise"));
                        e_o.appendChild(doc.createTextNode("AND NOT t" + tIdx + "." +
                                filter.property.getColumnRef() + " IS NULL\n"));
                        break;
                    case IN:
                        TypeMirror itemType = environment.getTypeUtils()
                                .erasure(((DeclaredType) filter.variableType).getTypeArguments().get(0));
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        org.w3c.dom.Element e_in = (org.w3c.dom.Element)
                                e.appendChild(doc.createElement("foreach"));
                        e_in.setAttribute("item", "item");
                        e_in.setAttribute("index", "index");
                        e_in.setAttribute("collection", filter.parameter);
                        e_in.setAttribute("open",
                                "AND t" + tIdx + "." + filter.property.getColumnRef() + " IN (");
                        e_in.setAttribute("close", ")");
                        e_in.setAttribute("separator", ",");
                        e_in.appendChild(doc.createTextNode("#{item" + ", javaType=" +
                                itemType + jdbcTypeParameterFor(itemType) + "}"));
                        break;
                    case LIKE_HAS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        org.w3c.dom.Element e_like = (org.w3c.dom.Element)
                                e.appendChild(doc.createElement("foreach"));
                        e_like.setAttribute("item", "item");
                        e_like.setAttribute("index", "index");
                        e_like.setAttribute("collection", filter.parameter);
                        e_like.setAttribute("open", "AND ");
                        e_like.setAttribute("close", "");
                        e_like.setAttribute("separator", " OR ");
                        e_like.appendChild(doc.createTextNode("t" +
                                tIdx + "." + filter.property.getColumnRef() +
                                " LIKE #{item" + ", javaType=" + filter.variableType +
                                jdbcTypeParameterFor(filter.variableType) + "}"));
                        break;
                    case LIKE_ALL_STARTS:
                    case LIKE_ALL_HAS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e_like = (org.w3c.dom.Element) e.appendChild(doc.createElement("foreach"));
                        e_like.setAttribute("item", "item");
                        e_like.setAttribute("index", "index");
                        e_like.setAttribute("collection", filter.parameter);
                        e_like.setAttribute("open", "AND ");
                        e_like.setAttribute("close", "");
                        e_like.setAttribute("separator", " AND ");
                        e_like.appendChild(doc.createTextNode("t" +
                                tIdx + "." + filter.property.getColumnRef() +
                                " LIKE #{item" + ", javaType=" + filter.variableType +
                                jdbcTypeParameterFor(filter.variableType) + "}"));
                        break;
                    case LIKE:
                    case LIKE_STARTS:
                    case LIKE_ENDS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " LIKE " + filterParameter + "\n"));
                        break;
                    case ILIKE:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " ILIKE " + filterParameter + "\n"));
                    default:
                        e = null;
                }

                if (e != null)
                    if (currentJoin != null && (!addJoins || !currentJoin.useInFieldsList)) {
                        IfExists ifExists = whereExists.get(currentJoin.property.name);
                        if (ifExists == null) {
                            org.w3c.dom.Element existsElt = doc.createElement("trim");
                            existsElt.setAttribute("prefix", "AND EXISTS (SELECT * FROM " +
                                    currentJoin.mappedBean.getTableRef() + " AS t" + tIdx + " WHERE ");
                            existsElt.setAttribute("prefixOverrides", "AND ");
                            existsElt.setAttribute("suffix", ")\n");
                            Mapping joinMap = builder.mappings.get(currentJoin.property.dataBean.interfaceType.toString() +
                                    "." + currentJoin.property.name);
                            existsElt.appendChild(doc.createTextNode(
                                    joinMap.stream().map(m -> "\nt" +
                                            currentJoin.mappedFromTableIndex +
                                            "." + m.property.getColumnRef() +
                                            " = t" + currentJoin.tableIndex +
                                            "." + m.mappedProperty.getColumnRef() + "\n")
                                            .collect(Collectors.joining(" AND "))
                            ));
                            ifExists = new IfExists(existsElt, filter.parameter);
                            whereExists.put(currentJoin.property.name, ifExists);
                        } else {
                            ifExists.parameters.add(filter.parameter);
                        }
                        ifExists.element.appendChild(e);
                    } else
                        where.add(e);
            }

        whereExists.values().forEach(ex -> {
            org.w3c.dom.Element elt = doc.createElement("if");
            elt.setAttribute("test",
                    ex.parameters.stream().map(p -> p + " != null").collect(Collectors.joining(" || ")));
            elt.appendChild(ex.element);
            where.add(elt);
        });
        if (addJoins) addJoins(mappedTables, sql);

        if (method.methodType == DDataMethodBuilder.MType.SELECT || method.methodType == DDataMethodBuilder.MType.GET) {
            domElement.appendChild(doc.createTextNode(sql.toString()));
            if (where.size() > 0) {
                org.w3c.dom.Element whereElt = doc.createElement("trim");
                whereElt.setAttribute("prefix", "WHERE");
                whereElt.setAttribute("prefixOverrides", "AND ");
                domElement.appendChild(whereElt);
                where.forEach(whereElt::appendChild);
            }
        } else {
            sql.append("WHERE\n");
            domElement.appendChild(doc.createTextNode(sql.toString()));
            where.forEach(domElement::appendChild);
        }

        if (order != null)
            addOrder(order, mappedTables.stream()
                    .filter(MappedTable::useInFieldsList)
                    .collect(Collectors.toList()), domElement);

        filters.stream().filter(f -> f.option == DDataFilterOption.LIMIT).findAny().ifPresent(limit -> {
            String limitParameter = "#{" + limit.parameter + ", javaType=" + (limit.variableType.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) limit.variableType) :
                    limit.variableType
            ) + jdbcTypeParameterFor(limit.variableType) + "}";

            org.w3c.dom.Element limitElt = (org.w3c.dom.Element)
                    domElement.appendChild(doc.createElement("if"));
            limitElt.setAttribute("test", limit.parameter + " != null");
            limitElt.appendChild(doc.createTextNode("LIMIT " + limitParameter + "\n"));
            filters.stream().filter(f -> f.option == DDataFilterOption.START).findAny().ifPresent(offset -> {
                String offsetParameter = "#{" + offset.parameter + ", javaType=" + (offset.variableType.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) offset.variableType) :
                        offset.variableType
                ) + jdbcTypeParameterFor(offset.variableType) + "}";

                org.w3c.dom.Element offsetElt = (org.w3c.dom.Element)
                        limitElt.appendChild(doc.createElement("if"));
                offsetElt.setAttribute("test", offset.parameter + " != null && " + offset.parameter + " != 0");
                offsetElt.appendChild(doc.createTextNode("OFFSET " + offsetParameter + "\n"));
            });
        });
    }

    private void addOrder(VariableElement order, List<MappedTable> tablesInSelect, org.w3c.dom.Element dynSql) {
        org.w3c.dom.Element ifElt = (org.w3c.dom.Element)
                dynSql.appendChild(dynSql.getOwnerDocument().createElement("if"));
        ifElt.setAttribute("test", order.getSimpleName() + " != null");
        ifElt.appendChild(dynSql.getOwnerDocument().createTextNode("\nORDER BY "));
        org.w3c.dom.Element forElt = (org.w3c.dom.Element)
                ifElt.appendChild(dynSql.getOwnerDocument().createElement("foreach"));
        forElt.setAttribute("item", "item");
        forElt.setAttribute("index", "index");
        forElt.setAttribute("collection", order.getSimpleName() + ".order");
        forElt.setAttribute("separator", ", ");
        forElt.appendChild(dynSql.getOwnerDocument().createTextNode("${item.attribute.columnName} ${item.order}"));
    }

    private void addJoins(Collection<MappedTable> joins, StringBuilder sql) {
        joins.stream()
                .filter(MappedTable::useInFieldsListOrFilters)
                .sorted(Comparator.comparingInt(t -> t.tableIndex))
                .forEach(join -> {
                    Mapping joinMap = builder.mappings.get(
                            join.property.dataBean.interfaceType.toString() + "." + join.property.name);
                    sql.append("LEFT JOIN ")
                            .append(join.mappedBean.getTableRef())
                            .append(" AS t").append(join.tableIndex)
                            .append(" ON (")
                            .append(joinMap.stream().map(m ->
                                    "t" + join.mappedFromTableIndex
                                            + "." + m.property.getColumnRef()
                                            + " = t" + join.tableIndex
                                            + "." + m.mappedProperty.getColumnRef()
                            ).collect(Collectors.joining(" AND ")));
                    DataBeanBuilder thisBean = joinMap.properties.get(0).dataBean;
                    DataBeanBuilder leftBean = joinMap.mappedProperties.get(0).dataBean;
                    if (leftBean.versionalType != null &&
                            environment.getTypeUtils().isSameType(thisBean.versionalType, leftBean.versionalType)) {

                        DataBeanPropertyBuilder thisVersionFrom =
                                thisBean.properties.values().stream().filter(p -> p.isVersionFrom)
                                        .findAny().orElse(null);
                        DataBeanPropertyBuilder leftVersionFrom =
                                leftBean.properties.values().stream().filter(p -> p.isVersionFrom)
                                        .findAny().orElse(null);
                        DataBeanPropertyBuilder leftVersionTo =
                                leftBean.properties.values().stream().filter(p -> p.isVersionTo)
                                        .findAny().orElse(null);

                        if (thisVersionFrom != null && leftVersionFrom != null && leftVersionTo != null) {
                            String parameter;
                            if (environment.getTypeUtils().directSupertypes(thisVersionFrom.type).stream()
                                    .anyMatch(c -> c.toString().equals(builder.temporalType.toString())) ||
                                    environment.getTypeUtils().isSubtype(thisVersionFrom.type, builder.oldDateType))
                                parameter = "CAST(" + buildSqlParameter(thisBean, thisVersionFrom) + " AS TIMESTAMP)";
                            else
                                parameter = buildSqlParameter(thisBean, thisVersionFrom);
                            sql
                                    .append("\n   AND t").append(join.tableIndex).append('.')
                                    .append(leftVersionFrom.columnName).append(" <= ").append(parameter)
                                    .append("\n   AND (t").append(join.tableIndex).append('.')
                                    .append(leftVersionTo.columnName).append(" > ").append(parameter)
                                    .append(" OR t").append(join.tableIndex).append('.')
                                    .append(leftVersionTo.columnName).append(" IS NULL)");
                        }
                    }

                    DataBeanPropertyBuilder discriminantProperty = leftBean.getDiscriminatorProperty();
                    if (discriminantProperty != null) {
                        sql.append("\n AND t").append(join.tableIndex).append('.').append('"')
                                .append(discriminantProperty.columnName).append("\" = ");
                        if ("java.lang.String".equals(discriminantProperty.type.toString()))
                            sql.append('\'').append(leftBean.getDiscriminatorValue()).append('\'');
                        else
                            sql.append(leftBean.getDiscriminatorValue());
                    }

                    sql.append(")\n");
                });
    }

    /**
     * not applicable for collection types
     *
     * @return string like #{name,javaType=...,jdbcType=...}
     */
    private String buildSqlParameter(DataBeanBuilder dataBean, DataBeanPropertyBuilder beanProperty) {
        Mapping mapping = builder.mappings.get(dataBean.interfaceType.toString() + "." + beanProperty.name);
        if (mapping != null) {
            TypeMirror mappedType = mapping.mappedProperties.get(0).type;
            return beanProperty.getColumnWriter("#{" + beanProperty.name + "_foreignKey, javaType=" + (mappedType.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) mappedType) : mappedType
            ) + jdbcTypeParameterFor(mappedType) + "}");
        } else
            return beanProperty.getColumnWriter("#{" + beanProperty.name + ", javaType=" + (beanProperty.type.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) beanProperty.type) :
                    beanProperty.type
            ) + jdbcTypeParameterFor(beanProperty.type) + "}");
    }

    private String jdbcTypeParameterFor(TypeMirror type) {
        String s = builder.jdbcTypeFor(type);
        return s.length() == 0 ? s : ", jdbcType=" + s;
    }

    private void addManagedBeanToFrom(StringBuilder sql, MappedTable mappedTable, FetchOptions fetchOptions) {
        String r = mappedTable.mappedBean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .filter(fetchOptions::filter4FieldsList)
                .filter(this::notManagedBean)
                .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                .map(p -> "  " + p.getColumnReader(mappedTable.tableIndex) +
                        " AS t" + mappedTable.tableIndex + "_" + p.columnName)
                .collect(Collectors.joining(",\n"));
        if (r.length() > 0) sql.append(",\n").append(r);
    }

    private void buildResultMap(
            org.w3c.dom.Element mapperRoot,
            DataRepositoryBuilder repository,
            String methodName,
            FetchOptions fetchOptions,
            List<MappedTable> mappedTables,
            List<FilterOption> filters
    ) {
        Document doc = mapperRoot.getOwnerDocument();
        MapBuilder map = new MapBuilder(methodName, repository);

        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());

        addPropertiesToResultMap(map, "", bean.properties.values(), fetchOptions);

        ArrayList<MappedTable> unmodifiableTables = new ArrayList<>(mappedTables);
        unmodifiableTables.stream()
                .filter(MappedTable::notSingleSmallDictionaryValue)
                .filter(mappedTable -> fetchOptions.filter4ResultMap(mappedTable.property))
                .filter(mappedTable -> !mappedTable.property.isCollectionOrMap())
                .filter(mappedTable -> repository.discriminator == null || bean.properties.values().stream()
                        .anyMatch(p -> mappedTable.mappedBean.interfaceType.toString().equals(p.mappedType.toString())))
                .forEach(mappedTable ->
                        addManagedBeanToResultMap(doc, map, mappedTable,
                                fetchOptions, repository, mappedTables,
                                fetchOptions.eagerTrunkLevel, filters,
                                false));
        unmodifiableTables.stream()
                .filter(mappedTable -> fetchOptions.filter4ResultMap(mappedTable.property))
                .filter(mappedTable -> mappedTable.property.isCollectionOrMap())
                .filter(mappedTable -> repository.discriminator == null || bean.properties.values().stream()
                        .anyMatch(p -> mappedTable.mappedBean.interfaceType.toString().equals(p.mappedType.toString())))
                .forEach(mappedTable ->
                        addManagedBeanToResultMap(doc, map, mappedTable,
                                fetchOptions, repository, mappedTables,
                                fetchOptions.eagerTrunkLevel, filters,
                                false));

        if (repository.discriminator != null) {
            unmodifiableTables = new ArrayList<>(mappedTables);
            for (DataRepositoryDiscriminator.Item item : repository.discriminator.beans) {
                DataBeanBuilder dbean = builder.beansByInterface.get(item.beanInterface);
                /*ArrayList<DataBeanPropertyBuilder> properties = new ArrayList<>();
                for (DataBeanPropertyBuilder prop : dbean.properties.values())
                    if (properties.stream().noneMatch(p -> prop.columnName.equals(p.columnName))) {
                        properties.add(prop);
                    }*/
                MapElement discriminatorMap = map.getDiscriminatorElement(item);
                addPropertiesToResultMap(discriminatorMap, "", dbean.properties.values(), fetchOptions);

                unmodifiableTables.stream()
                        .filter(MappedTable::notSingleSmallDictionaryValue)
                        .filter(mappedTable -> fetchOptions.filter4ResultMap(mappedTable.property))
                        .filter(mappedTable -> !mappedTable.property.isCollectionOrMap())
                        .filter(mappedTable -> dbean.properties.values().stream()
                                .anyMatch(p ->
                                        mappedTable.mappedBean.interfaceType.toString().equals(p.mappedType.toString())))
                        .forEach(mappedTable ->
                                addManagedBeanToResultMap(doc, discriminatorMap, mappedTable,
                                        fetchOptions, repository, mappedTables,
                                        fetchOptions.eagerTrunkLevel, filters,
                                        true));
                unmodifiableTables.stream()
                        .filter(mappedTable -> fetchOptions.filter4ResultMap(mappedTable.property))
                        .filter(mappedTable -> mappedTable.property.isCollectionOrMap())
                        .filter(mappedTable -> dbean.properties.values().stream()
                                .anyMatch(p -> mappedTable.mappedBean.interfaceType.toString().equals(p.mappedType.toString())))
                        .forEach(mappedTable ->
                                addManagedBeanToResultMap(doc, discriminatorMap, mappedTable,
                                        fetchOptions, repository, mappedTables,
                                        fetchOptions.eagerTrunkLevel, filters,
                                        true));
            }
        }
        map.write(mapperRoot);
    }

    private void addPropertiesToResultMap(
            MapElement map, String prefix,
            Collection<DataBeanPropertyBuilder> properties,
            FetchOptions fetchOptions
    ) {
        properties.stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .filter(p -> p.isId)
                .forEach(p -> map.addId(prefix, p));

        if (properties.iterator().next().dataBean.versionalType != null) map.addVersionalProperty();

        properties.stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .filter(p -> !(p.isId || p.isCollectionOrMap()))
                .filter(fetchOptions::filterIgnored)
                .filter(p -> !builder.beansByInterface.containsKey(p.type.toString()))
                .forEach(p -> map.addResult(prefix, p));
    }

    private void addManagedBeanToResultMap(
            Document doc,
            MapElement map,
            MappedTable mappedTable,
            FetchOptions fetchOptions,
            DataRepositoryBuilder repository,
            List<MappedTable> mappedTables,
            int trunkLevel,
            List<FilterOption> filters,
            boolean byDiscriminator
    ) {
        MapElement managed = map.createTableElement(mappedTable);

        Mapping mapping = builder.mappings.get(mappedTable.property.dataBean.interfaceType + "." + mappedTable.property.name);

        boolean lazy;
        if (mappedTable.property.isCollectionOrMap()) {
            DataBeanBuilder mappedBean = builder.beansByInterface.get(mappedTable.property.mappedType.toString());
            managed.setAttribute("ofType", mappedBean.getImplementationName());
            lazy = fetchOptions.fetchType != DDataFetchType.EAGER;
        } else {
            lazy = fetchOptions.fetchType == DDataFetchType.LAZY;
        }

        if (mapping != null && (lazy || trunkLevel < 1)) {
            if (!fetchOptions.truncateLazy) {
                map.addTable(managed);

                DataBeanBuilder thisBean = mapping.properties.get(0).dataBean;
                DataBeanBuilder mappedBean = mapping.mappedProperties.get(0).dataBean;
                DataBeanPropertyBuilder thisVersionFrom =
                        thisBean.properties.values().stream().filter(p -> p.isVersionFrom)
                                .findAny().orElse(null);
                DataBeanPropertyBuilder mappedVersionFrom =
                        mappedBean.properties.values().stream().filter(p -> p.isVersionFrom)
                                .findAny().orElse(null);
                DataBeanPropertyBuilder mappedVersionTo =
                        mappedBean.properties.values().stream().filter(p -> p.isVersionTo)
                                .findAny().orElse(null);

                StringBuilder columnsMapping = new StringBuilder();
                columnsMapping.append(mapping.properties.stream()
                        .filter(DataBeanPropertyBuilder::notIgnored)
                        .map(p -> p.columnName + "=" + (mappedTable.mappedFromTableIndex == 0 ? "" :
                                "t" + mappedTable.mappedFromTableIndex + "_") +
                                p.columnName)
                        .collect(Collectors.joining(",")));
                if (mappedBean.versionalType != null &&
                        environment.getTypeUtils().isSameType(thisBean.versionalType, mappedBean.versionalType)) {

                    if (thisVersionFrom != null && mappedVersionFrom != null && mappedVersionTo != null) {
                        columnsMapping.append(',').append(mappedVersionFrom.columnName).append("=").append("dDataBeanActualAt_");
                    }
                }
                managed.setAttribute("column", "{" + columnsMapping + "}");

                managed.setAttribute("fetchType", "lazy");

                String lazyLoadSelectId = "lazy_load_" + mappedBean.name +
                        "_" + mapping.mappedProperties.get(0).columnName;
                managed.setAttribute("select", lazyLoadSelectId);

                if (!repository.lazyLoads.containsKey(lazyLoadSelectId)) {

                    DataRepositoryBuilder beanRep =
                            builder.repositoriesByBean.get(mappedBean.interfaceType.toString());
                    org.w3c.dom.Element ll = doc.createElement("select");
                    ll.setAttribute("id", lazyLoadSelectId);
                    ll.setAttribute("resultMap", repository == beanRep ?
                            "get_ResultMap" : beanRep.mappingClassName + ".get_ResultMap");
                    org.w3c.dom.Element il = (org.w3c.dom.Element)
                            ll.appendChild(doc.createElement("include"));
                    il.setAttribute("refid", repository == beanRep ?
                            "get_select" : beanRep.mappingClassName + ".get_select");
                    StringBuilder sql = new StringBuilder();
                    sql.append("\nWHERE ").append(mapping.stream().map(m -> {
                        TypeMirror propType = m.manyToOne ?
                                m.property.type : m.mappedProperty.type;
                        return "t0." +
                                m.mappedProperty.getColumnRef() + " = " +
                                m.mappedProperty.getColumnWriter(
                                        "#{" + m.property.columnName +
                                                jdbcTypeParameterFor(propType) + "}");
                    }).collect(Collectors.joining(" AND "))).append("\n");

                    if (mappedBean.versionalType != null &&
                            environment.getTypeUtils().isSameType(thisBean.versionalType, mappedBean.versionalType))
                        if (thisVersionFrom != null && mappedVersionFrom != null && mappedVersionTo != null) {
                            String parameter = "#{" + mappedVersionFrom.columnName + "}";
                            sql.append("   AND t0.\"").append(mappedVersionFrom.columnName).append("\" <= ").append(parameter)
                                    .append("\n   AND (t0.\"")
                                    .append(mappedVersionTo.columnName).append("\" > ").append(parameter)
                                    .append(" OR t0.\"")
                                    .append(mappedVersionTo.columnName).append("\" IS NULL)\n");
                        }

                    ll.appendChild(doc.createTextNode(sql.toString()));
                    repository.lazyLoads.put(lazyLoadSelectId, ll);
                }
            }
        } else {
            map.addTable(managed);
            addPropertiesToResultMap(managed, "t" + mappedTable.tableIndex + "_",
                    mappedTable.mappedBean.properties.values(), fetchOptions);
            if (trunkLevel != 0) {
                List<DataBeanPropertyBuilder> mappedBeans = mappedTable.mappedBean.properties.values().stream()
                        .filter(DataBeanPropertyBuilder::notIgnored)
                        .filter(p -> !p.isId)
                        .filter(fetchOptions::filter4ResultMap)
                        .collect(Collectors.toList());

                mappedBeans.stream()
                        .filter(DataBeanPropertyBuilder::isSimple)
                        .forEach(mappedBean ->
                                addManaged2BeanToResultMap(doc, managed, mappedTable, mappedBean,
                                        fetchOptions, repository, mappedTables,
                                        !byDiscriminator ? trunkLevel - 1 : trunkLevel, filters)
                        );
                mappedBeans.stream()
                        .filter(DataBeanPropertyBuilder::isCollectionOrMap)
                        .forEach(mappedBean ->
                                addManaged2BeanToResultMap(doc, managed, mappedTable, mappedBean,
                                        fetchOptions, repository, mappedTables,
                                        !byDiscriminator ? trunkLevel - 1 : trunkLevel, filters)
                        );
            }
        }
    }

    private void addManaged2BeanToResultMap(
            Document doc,
            MapElement managed,
            MappedTable mappedTable,
            DataBeanPropertyBuilder mappedBean,
            FetchOptions fetchOptions,
            DataRepositoryBuilder repository,
            List<MappedTable> mappedTables,
            int trunkLevel,
            List<FilterOption> filters
    ) {
        DataBeanBuilder mapped2Bean = builder.beansByInterface.get(mappedBean.mappedType.toString());
        if (mapped2Bean != null) {
            MappedTable mapped2Table = new MappedTable(mappedTable.tableIndex, mappedTables.size() + 1,
                    mappedBean, mapped2Bean, fetchOptions, filters);
            if (mapped2Table.notSingleSmallDictionaryValue()) {
                mappedTables.add(mapped2Table);

                addManagedBeanToResultMap(doc, managed, mapped2Table, fetchOptions, repository, mappedTables,
                        trunkLevel, filters, false);
            }
        }
    }

    private void createDictionaryList(org.w3c.dom.Element mapperRoot, FetchOptions fetchOptions) {
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("select"));
        select.setAttribute("id", "dictionary");
        select.setAttribute("resultMap", "get_ResultMap");
        org.w3c.dom.Element include = (org.w3c.dom.Element)
                select.appendChild(doc.createElement("include"));
        include.setAttribute("refid", "get_select");
        if (!fetchOptions.order.isEmpty()) {
            select.appendChild(doc.createTextNode("ORDER BY " +
                    fetchOptions.order.stream()
                            .map(o -> o.getColumnRef() + " ASC")
                            .collect(Collectors.joining(", "))));
        }
    }

    class FilterOption {
        final DDataFilterOption option;
        final DataBeanPropertyBuilder property;
        final String parameter;
        final DataBeanPropertyBuilder mappedBy;
        final TypeMirror variableType;

        FilterOption(DataRepositoryBuilder repository, VariableElement variableElement) {
            DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName.toString());
            Optional<? extends AnnotationMirror> filterOpt = variableElement.getAnnotationMirrors().stream()
                    .filter(a -> a.toString().indexOf("_Filter_") > 0)
                    .findAny();

            parameter = variableElement.getSimpleName().toString();
            variableType = variableElement.asType();

            if (filterOpt.isPresent()) {
                AnnotationMirror filterMirror = filterOpt.get();
                Map<? extends ExecutableElement, ? extends AnnotationValue> filterProps =
                        environment.getElementUtils().getElementValuesWithDefaults(filterMirror);
                //System.out.println("filterMapped: " + filterProps);

                String value = filterProps.keySet().stream()
                        .filter(k -> "value".equals(k.getSimpleName().toString()))
                        .findAny()
                        .map(k -> filterProps.get(k).getValue().toString())
                        .orElse(null);
                DataBeanPropertyBuilder localProperty = value == null ? null :
                        bean.properties.values().stream()
                                .filter(p -> p.enumName.equals(value))
                                .findAny()
                                .orElse(null);

                DataBeanPropertyBuilder mapped = null;
                DataBeanPropertyBuilder mappedByProperty = null;
                for (DataBeanPropertyBuilder property : bean.properties.values()) {
                    String mapped_value = filterProps.keySet().stream()
                            .filter(k -> property.name.equals(k.getSimpleName().toString()))
                            .findAny()
                            .map(k -> filterProps.get(k).getValue().toString())
                            .orElse(null);
                    DataBeanBuilder mappedBean = mapped_value == null || "NONE_".equals(mapped_value) ? null :
                            builder.beansByInterface.get(property.mappedType.toString());
                    if (mappedBean != null) {
                        mapped = mappedBean.properties.values().stream()
                                .filter(p -> p.enumName.equals(mapped_value))
                                .findAny()
                                .orElse(null);
                        mappedByProperty = property;
                        break;
                    }
                }

                property = mapped != null ? mapped : localProperty;
                mappedBy = mapped != null ? mappedByProperty : localProperty;
                option = filterProps.keySet().stream()
                        .filter(k -> "option".equals(k.getSimpleName().toString()))
                        .findAny()
                        .map(k -> DDataFilterOption.valueOf(
                                filterProps.get(k).getValue().toString()
                        ))
                        .orElse(DDataFilterOption.EQUALS);
            } else {
                option = null;
                property = null;
                mappedBy = null;
            }
        }
    }

    private class FetchOptions {
        final DDataFetchType fetchType;
        final String sqlSelect;
        final String resultMap;
        final List<DataBeanPropertyBuilder> ignore;
        final int eagerTrunkLevel;
        final boolean truncateLazy;
        final List<DataBeanPropertyBuilder> order;

        FetchOptions(DataRepositoryBuilder repository, AnnotationMirror fetchMirror) {
            DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName.toString());
            Map<? extends ExecutableElement, ? extends AnnotationValue> fetchProps =
                    environment.getElementUtils().getElementValuesWithDefaults(fetchMirror);

            //DDataFetchType
            String value = fetchProps.keySet().stream()
                    .filter(k -> "value".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue().toString())
                    .orElse(null);
            fetchType = value == null ?
                    DDataFetchType.COLLECTIONS_ARE_LAZY : DDataFetchType.valueOf(value);

            Object ignoreObj = fetchProps.keySet().stream()
                    .filter(k -> "ignore".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue())
                    .orElse(null);
            final List<Object> ignoreList = (ignoreObj != null && ignoreObj instanceof List) ?
                    (List) ignoreObj : Collections.emptyList();
            ignore = ignoreList.stream()
                    .map(Object::toString)
                    .map(s -> {
                        int i = s.lastIndexOf('.');
                        if (i > 0) return s.substring(i + 1);
                        else return s;
                    })
                    .map(name -> bean.properties.values().stream()
                            .filter(p -> p.enumName.equals(name))
                            .findAny().orElse(null))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            Object orderObj = fetchProps.keySet().stream()
                    .filter(k -> "defaultOrder".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue())
                    .orElse(null);
            final List<Object> orderList = (orderObj != null && orderObj instanceof List) ?
                    (List) orderObj : Collections.emptyList();
            order = orderList.stream()
                    .map(Object::toString)
                    .map(s -> {
                        int i = s.lastIndexOf('.');
                        if (i > 0) return s.substring(i + 1);
                        else return s;
                    })
                    .map(name -> bean.properties.values().stream()
                            .filter(p -> p.enumName.equals(name))
                            .findAny().orElse(null))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            sqlSelect = fetchProps.keySet().stream()
                    .filter(k -> "select".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue().toString())
                    .orElse("");

            resultMap = fetchProps.keySet().stream()
                    .filter(k -> "resultMap".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue().toString())
                    .orElse("");

            eagerTrunkLevel = fetchProps.keySet().stream()
                    .filter(k -> "eagerTrunkLevel".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> Integer.parseInt(fetchProps.get(k).getValue().toString()))
                    .orElse(1);

            truncateLazy = fetchProps.keySet().stream()
                    .filter(k -> "truncateLazy".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> Boolean.parseBoolean(fetchProps.get(k).getValue().toString()))
                    .orElse(false);
        }

        FetchOptions(int trunkLevel) {
            fetchType = DDataFetchType.COLLECTIONS_ARE_LAZY;
            sqlSelect = "";
            resultMap = "";
            ignore = Collections.emptyList();
            eagerTrunkLevel = trunkLevel;
            truncateLazy = false;
            order = Collections.emptyList();
        }

        boolean filterIgnored(DataBeanPropertyBuilder property) {
            return !this.ignore.contains(property);
        }

        boolean filter4ResultMap(DataBeanPropertyBuilder property) {
            return !(this.fetchType == DDataFetchType.NO ||
                    this.ignore.contains(property) ||
                    (this.fetchType == DDataFetchType.COLLECTIONS_ARE_NO && property.isCollectionOrMap())
            );
        }

        boolean filter4FieldsList(DataBeanPropertyBuilder property) {
            return !(this.fetchType == DDataFetchType.NO ||
                    this.ignore.contains(property) ||
                    ((
                            this.fetchType == DDataFetchType.COLLECTIONS_ARE_LAZY ||
                                    this.fetchType == DDataFetchType.COLLECTIONS_ARE_NO
                    ) && property.isCollectionOrMap()) ||
                    (this.fetchType == DDataFetchType.LAZY &&
                            builder.beansByInterface.containsKey(property.mappedType.toString())
                    )
            );
        }
    }

    class MappedTable {
        final int tableIndex;
        final int mappedFromTableIndex;
        final DataBeanPropertyBuilder property;
        final DataBeanBuilder mappedBean;
        final boolean singleSmallDictionaryValue;
        final boolean useInFieldsList;
        final boolean useInFilters;

        MappedTable(int fromTableIndex, int tableIndex,
                    DataBeanPropertyBuilder p,
                    DataBeanBuilder b,
                    FetchOptions fetchOptions,
                    List<FilterOption> filters
        ) {
            this.tableIndex = tableIndex;
            this.mappedFromTableIndex = fromTableIndex;
            this.property = p;
            this.mappedBean = b;
            this.singleSmallDictionaryValue = b.dictionary == DictionaryType.SMALL && !p.isCollectionOrMap();
            this.useInFieldsList = !singleSmallDictionaryValue && fetchOptions.filter4FieldsList(p);
            this.useInFilters = fromTableIndex < 2 && filters != null && filters.stream()
                    .anyMatch(f -> f.property != null && f.property.name.equals(p.name) &&
                            f.property.dataBean.getImplementationName().equals(p.dataBean.getImplementationName())
                    );
        }

        boolean useInFieldsList() {
            return useInFieldsList;
        }

        boolean useInFieldsListOrFilters() {
            return useInFilters || useInFieldsList;
        }

        boolean notSingleSmallDictionaryValue() {
            return !singleSmallDictionaryValue;
        }
    }

    private class IfExists {
        ArrayList<String> parameters = new ArrayList<>();
        org.w3c.dom.Element element;

        IfExists(org.w3c.dom.Element element, String parameter) {
            this.element = element;
            this.parameters.add(parameter);
        }
    }
}
