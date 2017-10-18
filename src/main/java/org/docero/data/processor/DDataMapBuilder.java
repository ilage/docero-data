package org.docero.data.processor;

import org.docero.data.DDataFetchType;
import org.docero.data.DDataFilterOption;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
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
    private final TypeMirror temporalType;
    private final TypeMirror oldDateType;
    private final HashSet<String> userMappingFiles = new HashSet<>();

    DDataMapBuilder(DDataBuilder builder, ProcessingEnvironment environment) {
        this.builder = builder;
        this.environment = environment;
        temporalType = environment.getElementUtils().getTypeElement("java.time.temporal.Temporal").asType();
        oldDateType = environment.getElementUtils().getTypeElement("java.util.Date").asType();
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
            FetchOptions defaultFetchOptions = new FetchOptions(-1);
            if (repositoryElement == null) {
                repositoryNamespace = repository.forInterfaceName();
                createSimpleGet(mapperRoot, repository, defaultFetchOptions);
                createSimpleInsert(mapperRoot, repository, defaultFetchOptions);
                createSimpleUpdate(mapperRoot, repository, defaultFetchOptions);
                createSimpleDelete(mapperRoot, repository, defaultFetchOptions);
            } else {
                repositoryNamespace = repository.repositoryInterface.toString();

                if (repository.defaultGetMethod == null) createSimpleGet(mapperRoot, repository, defaultFetchOptions);
                if (!repository.hasInsert) createSimpleInsert(mapperRoot, repository, defaultFetchOptions);
                if (!repository.hasUpdate) createSimpleUpdate(mapperRoot, repository, defaultFetchOptions);
                if (repository.defaultDeleteMethod == null)
                    createSimpleDelete(mapperRoot, repository, defaultFetchOptions);

                //System.out.println(repository.repositoryInterface + ":" + repositoryElement.getEnclosedElements());
                for (Element methodElement : repositoryElement.getEnclosedElements())
                    if (methodElement.getKind() == ElementKind.METHOD && !methodElement.getModifiers().contains(Modifier.STATIC) && repository.isCreatedByInterface ?
                            !methodElement.getModifiers().contains(Modifier.DEFAULT) :
                            methodElement.getModifiers().contains(Modifier.ABSTRACT))
                        createDefinedMethod(mapperRoot, (ExecutableElement) methodElement, repository);
            }
            repository.lazyLoads.values().forEach(mapperRoot::appendChild);
            mapperRoot.setAttribute("namespace", repositoryNamespace);

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

        if (builder.spring)
            try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.DDataConfiguration")) {
                cf.println("package org.docero.data;");
                for (String pkg : builder.packages) cf.println("import " + pkg + ".*;");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.println("@org.springframework.context.annotation.Configuration");
                cf.startBlock("public class DDataConfiguration {");
                for (DataRepositoryBuilder repository : builder.repositories) {
                    int offset = repository.daoClassName.lastIndexOf('.') + 1;
                    String methodName = Character.toLowerCase(repository.daoClassName.charAt(offset)) +
                            repository.daoClassName.substring(offset + 1);
                    cf.println("@org.springframework.context.annotation.Bean");
                    cf.startBlock("public " + repository.repositoryInterface + " " + methodName +
                            "(org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory) {");
                    DeclaredType getType = environment.getTypeUtils().getDeclaredType(
                            environment.getElementUtils().getTypeElement("org.docero.data.DDataRepository"),
                            repository.forInterfaceName, repository.idClass);

                    cf.println(getType + " r = DData.getRepository(" + repository.forInterfaceName + ".class);");
                    cf.println("if (r != null)\n                " +
                            "((org.mybatis.spring.support.SqlSessionDaoSupport) r).setSqlSessionFactory(sqlSessionFactory);");
                    cf.println("return (" + repository.repositoryInterface + ") r;");
                    cf.endBlock("}");
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
                cf.endBlock("}");
            }

        return true;
    }

    private void createDefinedMethod(
            org.w3c.dom.Element mapperRoot,
            ExecutableElement methodElement,
            DataRepositoryBuilder repository
    ) throws Exception {
        TypeMirror returnType = methodElement.getReturnType();
        Optional<DDataMethodBuilder> methodOpt = repository.methods.stream().filter(m ->
                m.methodName.equals(methodElement.getSimpleName().toString()) && (
                        (m.returnType == null && returnType == null) ||
                                (m.returnType != null && m.returnType.toString().equals(returnType.toString()))
                ) &&
                        m.parameters.size() == methodElement.getParameters().size()/* &&
                        m.parameters.stream()
                                .map(p -> environment.getTypeUtils().erasure(p.type).toString())
                                .map(p -> p.lastIndexOf('.') > 0 ? p.substring(p.lastIndexOf('.')) : p)
                                .collect(Collectors.joining("|")).equals(
                                methodElement.getParameters().stream()
                                        .map(p -> environment.getTypeUtils().erasure(p.asType()).toString())
                                        .map(p -> p.lastIndexOf('.') > 0 ? p.substring(p.lastIndexOf('.')) : p)
                                        .collect(Collectors.joining("|"))
                        )*/
        ).findAny();

        if (methodOpt.isPresent()) {
            Document doc = mapperRoot.getOwnerDocument();
            DDataMethodBuilder method = methodOpt.get();
            DataBeanBuilder bean = builder.beansByInterface.get(method.repositoryBuilder.forInterfaceName());

            FetchOptions fetchOptions = methodElement.getAnnotationMirrors().stream()
                    .filter(a -> a.toString().indexOf("_DDataFetch_") > 0)
                    .findAny().map(f -> new FetchOptions(repository, f))
                    .orElse(new FetchOptions(-1));

            ArrayList<FilterOption> filters = new ArrayList<>();
            VariableElement order = null;
            for (VariableElement variableElement : methodElement.getParameters()) {
                FilterOption filter = new FilterOption(repository, variableElement);
                if (filter.option != null) filters.add(filter);
                else if (variableElement.asType().toString().startsWith("org.docero.data.DDataOrder")) {
                    order = variableElement;
                }
            }

            AtomicInteger index = new AtomicInteger();
            CopyOnWriteArrayList<MappedTable> mappedTables = new CopyOnWriteArrayList<>(bean.properties.values().stream()
                    .map(p -> {
                        DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                        return b == null ? null :
                                new MappedTable(0, index.incrementAndGet(), p, b, fetchOptions, filters);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));

            String methodName = method == repository.defaultGetMethod || method == repository.defaultDeleteMethod ?
                    method.methodName :
                    method.methodName + "_" + method.methodIndex;
            switch (method.methodType) {
                case SELECT:
                case GET:
                    if (fetchOptions.resultMap.length() == 0 && !method.returnSimpleType)
                        buildResultMap(mapperRoot, method.repositoryBuilder, methodName, fetchOptions, mappedTables, filters);

                    org.w3c.dom.Element select = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    select.setAttribute("id", methodName);
                    select.setAttribute("parameterType", "HashMap");
                    if (method.returnSimpleType)
                        select.setAttribute("resultType", method.returnType.toString());
                    else if (fetchOptions.resultMap.length() == 0)
                        select.setAttribute("resultMap", methodName + "_ResultMap");
                    else {
                        select.setAttribute("resultMap", fetchOptions.resultMap);
                        userMappingFiles.add(fetchOptions.resultMap.substring(0, fetchOptions.resultMap.lastIndexOf('.')));
                    }
                    buildSql(repository, method, bean, fetchOptions, mappedTables, select, filters, order, false);
                    break;
                case INSERT:
                    org.w3c.dom.Element insert = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("insert"));
                    insert.setAttribute("id", methodName);
                    insert.setAttribute("parameterType", "HashMap");
                    buildSql(repository, method, bean, fetchOptions, mappedTables, insert, filters, order, false);
                    break;
                case UPDATE:
                    org.w3c.dom.Element update = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("update"));
                    update.setAttribute("id", methodName);
                    update.setAttribute("parameterType", "HashMap");
                    buildSql(repository, method, bean, fetchOptions, mappedTables, update, filters, order, false);
                    break;
                default: //case DELETE:
                    org.w3c.dom.Element delete = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    delete.setAttribute("id", methodName);
                    delete.setAttribute("parameterType", "HashMap");
                    buildSql(repository, method, bean, fetchOptions, mappedTables, delete, filters, order, false);
            }
        } else
            throw new Exception("not found info about method '" + methodElement.getSimpleName() +
                    " of " + repository.repositoryInterface);
    }

    private void buildSql(
            DataRepositoryBuilder repository,
            DDataMethodBuilder method,
            DataBeanBuilder bean,
            FetchOptions fetchOptions,
            List<MappedTable> mappedTables,
            org.w3c.dom.Element domElement,
            ArrayList<FilterOption> filters,
            VariableElement order,
            boolean defaultGet) {
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
                                jdbcTypeFor(pType, environment) +
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
                    sql.append(bean.properties.values().stream()
                            .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                            .filter(this::notManagedBean)
                            .filter(fetchOptions::filterIgnored)
                            .map(p -> "  t0." + p.getColumnRef() + " AS " + p.getColumnRef())
                            .collect(Collectors.joining(",\n")));
                    if (fetchOptions.fetchType != DDataFetchType.NO)
                        mappedTables.stream().filter(MappedTable::useInFieldsList).forEach(t ->
                                addManagedBeanToFrom(sql, t, fetchOptions));
                    if (!limitedSelect) sql.append("\nFROM ").append(bean.getTableRef()).append(" AS t0\n");
                    break;
                case INSERT:
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
                    }
                    bean.properties.values().stream()
                            .filter(DataBeanPropertyBuilder::isGenerated)
                            .forEach(prop ->
                            {
                                org.w3c.dom.Element sk = (org.w3c.dom.Element)
                                        domElement.appendChild(doc.createElement("selectKey"));
                                sk.setAttribute("keyProperty", prop.columnName);
                                sk.setAttribute("resultType", prop.type.toString());
                                sk.setAttribute("statementType", "PREPARED");
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
                            });
                    sql.append("\nINSERT INTO ").append(bean.getTableRef()).append(" (");
                    sql.append(bean.properties.values().stream()
                            .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                            .filter(this::notManagedBean)
                            .map(DataBeanPropertyBuilder::getColumnRef)
                            .collect(Collectors.joining(", ")));
                    sql.append(")\n");
                    sql.append("VALUES (\n");
                    sql.append(bean.properties.values().stream()
                            .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                            .filter(this::notManagedBean)
                            .map(p -> buildSqlParameter(bean, p))
                            .collect(Collectors.joining(",\n")));
                    sql.append("\n)\n");
                    break;
                case UPDATE:
                    sql.append("\nUPDATE ").append(bean.getTableRef()).append(" AS t0 SET\n");
                    sql.append(bean.properties.values().stream()
                            .filter(DataBeanPropertyBuilder::notId)
                            .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                            .filter(this::notManagedBean)
                            .map(p -> p.getColumnRef() + " = " + buildSqlParameter(bean, p))
                            .collect(Collectors.joining(",\n")))
                            .append("\n");
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
                        if (defaultGet || method == repository.defaultGetMethod) {
                            buildSelect4DefaultGet(domElement, sql);
                            domElement.appendChild(doc.createTextNode("\nWHERE " +
                                    filters.stream()
                                            .filter(f -> f.property != null)
                                            .map(f -> f.property.getColumnRef() + " = " + buildSqlParameter(bean, f.property))
                                            .collect(Collectors.joining(" AND ")) +
                                    "\n"));
                        } else
                            addFiltersToSql(domElement, sql, bean, method, mappedTables, filters, order, true);
                    } else {
                        addJoins(mappedTables, sql);
                        StringBuilder ssql;
                        if (defaultGet || method == repository.defaultGetMethod) {
                            buildSelect4DefaultGet(domElement, sql);
                            ssql = new StringBuilder("\n");
                        } else ssql = sql;

                        ssql.append("WHERE ");
                        if (repository.versionalType == null) {
                            ssql.append(bean.properties.values().stream()
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
                                    .filter(MappedTable::useInFieldsList)
                                    .collect(Collectors.toList()), domElement);
                    }
            }
        }
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
            ArrayList<FilterOption> filters,
            VariableElement order,
            boolean addJoins
    ) {
        Document doc = domElement.getOwnerDocument();

        List<org.w3c.dom.Element> where = new ArrayList<>();

        HashSet<MappedTable> joins = new HashSet<>();
        joins.addAll(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()));

        HashMap<String, IfExists> whereExists = new HashMap<>();
        org.w3c.dom.Element e;
        for (FilterOption filter : filters)
            if (filter.option != null && filter.property != null) {
                Optional<MappedTable> table = mappedTables.stream()
                        .filter(mb -> mb.mappedFromTableIndex <= 1 || filter.property.dataBean != bean)
                        .filter(mb -> mb.property.dataBean == filter.mappedBy.dataBean)
                        //.filter(mb -> mb.mappedByProperty.equals(filter.mappedBy.name))
                        .filter(mb -> mb.property.name.equals(filter.mappedBy.name)
                        ).findAny();
                table.ifPresent(joins::add);
                int tIdx = table.map(mb -> mb.tableIndex).orElse(0);
                MappedTable currentJoin = table.orElse(null);

                String filterParameter = "#{" + filter.parameter + ", javaType=" + (filter.variableType.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) filter.variableType) :
                        filter.variableType
                ) + jdbcTypeFor(filter.variableType, environment) + "}";

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
                        e_w.setAttribute("test", filter.parameter + " == TRUE");
                        e_w.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " IS NULL\n"));

                        org.w3c.dom.Element e_o = (org.w3c.dom.Element)
                                e_c.appendChild(doc.createElement("otherwise"));
                        e_o.appendChild(doc.createTextNode("AND NOT t" + tIdx + "." +
                                filter.property.getColumnRef() + " IS NULL\n"));
                        break;
                    case IN:
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
                        TypeMirror itemType = environment.getTypeUtils()
                                .erasure(((DeclaredType) filter.variableType).getTypeArguments().get(0));
                        e_in.appendChild(doc.createTextNode("#{item" + ", javaType=" +
                                itemType + jdbcTypeFor(itemType, environment) + "}"));
                        break;
                    case LIKE:
                    case LIKE_HAS:
                    case LIKE_STARTS:
                    case LIKE_ENDS:
                    case LIKE_ALL_STARTS:
                    case LIKE_ALL_HAS:
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
                                            "." + m.properties.get(0).getColumnRef() +
                                            " = t" + currentJoin.tableIndex +
                                            "." + m.mappedProperties.get(0).getColumnRef() + "\n")
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

        if (method.methodType == DDataMethodBuilder.MType.SELECT) {
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
            ) + jdbcTypeFor(limit.variableType, environment) + "}";

            org.w3c.dom.Element limitElt = (org.w3c.dom.Element)
                    domElement.appendChild(doc.createElement("if"));
            limitElt.setAttribute("test", limit.parameter + " != null");
            limitElt.appendChild(doc.createTextNode("LIMIT " + limitParameter + "\n"));
            filters.stream().filter(f -> f.option == DDataFilterOption.START).findAny().ifPresent(offset -> {
                String offsetParameter = "#{" + offset.parameter + ", javaType=" + (offset.variableType.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) offset.variableType) :
                        offset.variableType
                ) + jdbcTypeFor(offset.variableType, environment) + "}";

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
                .filter(t -> t.useInFieldsList || t.useInFilters)
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
                                            + "." + m.properties.get(0).getColumnRef()
                                            + " = t" + join.tableIndex
                                            + "." + m.mappedProperties.get(0).getColumnRef()
                            ).collect(Collectors.joining(" AND ")))
                            .append(")\n");
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
            return "#{" + beanProperty.name + "_foreignKey, javaType=" + (mappedType.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) mappedType) : mappedType
            ) + jdbcTypeFor(mappedType, environment) + "}";
        } else
            return "#{" + beanProperty.name + ", javaType=" + (beanProperty.type.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) beanProperty.type) :
                    beanProperty.type
            ) + jdbcTypeFor(beanProperty.type, environment) + "}";
    }

    private String jdbcTypeFor(TypeMirror type, ProcessingEnvironment environment) {
        String s = type.toString();

        if (java.time.LocalDate.class.getCanonicalName().equals(s)
                ) return ", jdbcType=DATE";

        if (java.time.LocalTime.class.getCanonicalName().equals(s)
                ) return ", jdbcType=TIME";

        if (//not work environment.getTypeUtils().isSubtype(type, temporalType) ||
                environment.getTypeUtils().directSupertypes(type).stream()
                        .anyMatch(c -> c.toString().equals(temporalType.toString())) ||
                        environment.getTypeUtils().isSubtype(type, oldDateType)
                ) return ", jdbcType=TIMESTAMP";

        if (String.class.getCanonicalName().equals(s))
            return ", jdbcType=VARCHAR";

        if ("short".equals(s) ||
                java.lang.Short.class.getCanonicalName().equals(s)
                ) return ", jdbcType=SMALLINT";

        if ("int".equals(s) ||
                java.lang.Integer.class.getCanonicalName().equals(s)
                ) return ", jdbcType=INTEGER";

        if ("long".equals(s) ||
                java.lang.Long.class.getCanonicalName().equals(s)
                ) return ", jdbcType=BIGINT";

        if ("float".equals(s) ||
                java.lang.Float.class.getCanonicalName().equals(s)
                ) return ", jdbcType=REAL";

        if ("double".equals(s) ||
                java.lang.Double.class.getCanonicalName().equals(s)
                ) return ", jdbcType=DOUBLE";

        if (java.math.BigInteger.class.getCanonicalName().equals(s) ||
                java.math.BigDecimal.class.getCanonicalName().equals(s)
                ) return ", jdbcType=NUMERIC";

        return "";
    }

    private void addManagedBeanToFrom(StringBuilder sql, MappedTable mappedTable, FetchOptions fetchOptions) {
        String r = mappedTable.mappedBean.properties.values().stream()
                .filter(fetchOptions::filter4FieldsList)
                .filter(this::notManagedBean)
                .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                .map(p -> "  t" + mappedTable.tableIndex + "." + p.getColumnRef() +
                        " AS t" + mappedTable.tableIndex + "_" + p.columnName)
                .collect(Collectors.joining(",\n"));
        if (r.length() > 0) sql.append(",\n").append(r);
    }

    private void buildResultMap(
            org.w3c.dom.Element mapperRoot,
            DataRepositoryBuilder repository, String methodName,
            FetchOptions fetchOptions,
            CopyOnWriteArrayList<MappedTable> mappedTables,
            ArrayList<FilterOption> filters
    ) {
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element map = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("resultMap"));
        map.setAttribute("id", methodName + "_ResultMap");
        map.setAttribute("type", repository.beanImplementation);

        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());

        addPropertiesToResultMap(map, "", bean.properties.values(), fetchOptions);

        mappedTables.stream()
                .filter(b -> fetchOptions.filter4ResultMap(b.property))
                .filter(b -> !b.property.isCollectionOrMap())
                .forEach(b ->
                        addManagedBeanToResultMap(map, b,
                                fetchOptions, repository, mappedTables,
                                fetchOptions.eagerTrunkLevel, filters));
        mappedTables.stream()
                .filter(b -> fetchOptions.filter4ResultMap(b.property))
                .filter(b -> b.property.isCollectionOrMap())
                .forEach(b ->
                        addManagedBeanToResultMap(map, b,
                                fetchOptions, repository, mappedTables,
                                fetchOptions.eagerTrunkLevel, filters));
    }

    private void addPropertiesToResultMap(
            org.w3c.dom.Element element, String prefix,
            Collection<DataBeanPropertyBuilder> properties,
            FetchOptions fetchOptions) {
        Document doc = element.getOwnerDocument();
        properties.stream()
                .filter(p -> p.isId)
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            element.appendChild(doc.createElement("id"));
                    id.setAttribute("property", p.name);
                    id.setAttribute("column", prefix + p.columnName);
                });

        properties.stream()
                .filter(p -> !(p.isId || p.isCollectionOrMap() || p.columnName == null))
                .filter(fetchOptions::filterIgnored)
                .filter(p -> !builder.beansByInterface.containsKey(p.type.toString()))
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            element.appendChild(doc.createElement("result"));
                    id.setAttribute("property", p.name);
                    id.setAttribute("column", prefix + p.columnName);
                });
    }

    private void addManagedBeanToResultMap(
            org.w3c.dom.Element map,
            MappedTable mappedTable,
            FetchOptions fetchOptions,
            DataRepositoryBuilder repository,
            List<MappedTable> mappedTables,
            int trunkLevel,
            List<FilterOption> filters
    ) {
        Document doc = map.getOwnerDocument();
        org.w3c.dom.Element managed = (org.w3c.dom.Element) map.appendChild(doc.createElement(
                mappedTable.property.isCollectionOrMap() ? "collection" : "association"));
        managed.setAttribute("property", mappedTable.property.name);
        managed.setAttribute("javaType", mappedTable.property.isCollection ? "ArrayList" : (
                mappedTable.property.isMap ? "HashMap" : mappedTable.mappedBean.getImplementationName())
        );
        Mapping mapping = builder.mappings.get(mappedTable.property.dataBean.interfaceType + "." + mappedTable.property.name);
        boolean lazy;
        if (mappedTable.property.isCollectionOrMap()) {
            DataBeanBuilder mappedBean = this.builder.beansByInterface.get(mappedTable.property.mappedType.toString());
            managed.setAttribute("ofType", mappedBean.getImplementationName());
            lazy = fetchOptions.fetchType != DDataFetchType.EAGER;
        } else {
            lazy = fetchOptions.fetchType == DDataFetchType.LAZY;
        }

        if (mapping != null && (lazy || trunkLevel < 1)) {
            if (!fetchOptions.truncateLazy) {
                if (mapping.properties.size() == 1) {
                    managed.setAttribute("column", (mappedTable.mappedFromTableIndex == 0 ? "" :
                            "t" + mappedTable.mappedFromTableIndex + "_") +
                            mapping.properties.get(0).columnName);
                    managed.setAttribute("foreignColumn", mapping.mappedProperties.get(0).columnName);
                } else {
                    managed.setAttribute("column", "{" + mapping.properties.stream()
                            .map(p -> p.columnName + "=" + (mappedTable.mappedFromTableIndex == 0 ? "" :
                                    "t" + mappedTable.mappedFromTableIndex + "_") +
                                    p.columnName)
                            .collect(Collectors.joining(",")) +
                            "}");
                    managed.setAttribute("foreignColumn", mapping.mappedProperties.stream()
                            .map(p -> p.columnName + "=" + p.columnName)
                            .collect(Collectors.joining(",")));
                }
                managed.setAttribute("fetchType", "lazy");

                DataBeanBuilder mappedBean = mapping.mappedProperties.get(0).dataBean;
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
                    ll.appendChild(doc.createTextNode("\nWHERE " +
                            mapping.stream().map(m -> {
                                TypeMirror propType = m.manyToOne ?
                                        m.properties.get(0).type : m.mappedProperties.get(0).type;
                                return "t0." +
                                        m.mappedProperties.get(0).getColumnRef() + " = " +
                                        "#{" + m.properties.get(0).columnName +
                                        jdbcTypeFor(propType, environment) + "}";
                            }).collect(Collectors.joining(" AND ")) +
                            "\n"));

                    repository.lazyLoads.put(lazyLoadSelectId, ll);
                }
            }
        } else {
            addPropertiesToResultMap(managed, "t" + mappedTable.tableIndex + "_",
                    mappedTable.mappedBean.properties.values(), fetchOptions);
            if (trunkLevel != 0) {
                List<DataBeanPropertyBuilder> mappedBeans = mappedTable.mappedBean.properties.values().stream()
                        .filter(p -> !p.isId)
                        .filter(fetchOptions::filter4ResultMap)
                        .collect(Collectors.toList());

                mappedBeans.stream()
                        .filter(DataBeanPropertyBuilder::isSimple)
                        .forEach(mappedBean ->
                                addManaged2BeanToResultMap(managed, mappedTable, mappedBean,
                                        fetchOptions, repository, mappedTables, trunkLevel - 1, filters)
                        );
                mappedBeans.stream()
                        .filter(DataBeanPropertyBuilder::isCollectionOrMap)
                        .forEach(mappedBean ->
                                addManaged2BeanToResultMap(managed, mappedTable, mappedBean,
                                        fetchOptions, repository, mappedTables, trunkLevel - 1, filters)
                        );
            }
        }
    }

    private void addManaged2BeanToResultMap(
            org.w3c.dom.Element managed,
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
            mappedTables.add(mapped2Table);

            addManagedBeanToResultMap(managed, mapped2Table, fetchOptions, repository, mappedTables,
                    trunkLevel == -1 ? -2 : trunkLevel - 1, filters);
        }
    }

    private void createSimpleDelete(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("delete"));
        select.setAttribute("id", "delete");

        select.setAttribute("parameterType", bean.keyType);
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.DELETE);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, null, false);
    }

    private void createSimpleUpdate(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("update"));
        select.setAttribute("id", "update");

        select.setAttribute("parameterType", bean.getImplementationName());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.UPDATE);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, null, false);
    }

    private void createSimpleInsert(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("insert"));
        select.setAttribute("id", "insert");

        select.setAttribute("parameterType", bean.getImplementationName());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.INSERT);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, null, false);
    }

    private void createSimpleGet(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        AtomicInteger index = new AtomicInteger();
        CopyOnWriteArrayList<MappedTable> mappedTables = new CopyOnWriteArrayList<>(bean.properties.values().stream()
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                    return b == null ? null : new MappedTable(0, index.incrementAndGet(), p, b, fetchOptions, null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        Document doc = mapperRoot.getOwnerDocument();

        buildResultMap(mapperRoot, repository, "get", fetchOptions, mappedTables, null);

        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("select"));
        select.setAttribute("id", "get");
        select.setAttribute("resultMap", "get_ResultMap");

        select.setAttribute("parameterType", bean.keyType);
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.GET);
        buildSql(repository, method, bean, fetchOptions, mappedTables, select, null, null, true);
    }

    private class FilterOption {
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

    private class MappedTable {
        final int tableIndex;
        final int mappedFromTableIndex;
        final DataBeanPropertyBuilder property;
        final DataBeanBuilder mappedBean;
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
            this.useInFieldsList = fetchOptions.filter4FieldsList(p);
            this.useInFilters = fromTableIndex < 2 && filters != null && filters.stream()
                    .anyMatch(f -> f.property != null && f.property.name.equals(p.name) &&
                            f.property.dataBean.getImplementationName().equals(p.dataBean.getImplementationName())
                    );
        }

        boolean useInFieldsList() {
            return useInFieldsList;
        }

        boolean useInFieldsListOrFilters() {
            return useInFilters;
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
