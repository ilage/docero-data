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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DDataMapBuilder {
    private final DDataBuilder builder;
    private final ProcessingEnvironment environment;
    private final HashMap<String, Mapping> mappings = new HashMap<>();
    private final TypeMirror temporalType;
    private final TypeMirror oldDateType;
    private final HashSet<String> userMappingFiles = new HashSet<>();

    DDataMapBuilder(DDataBuilder builder, ProcessingEnvironment environment) {
        this.builder = builder;
        this.environment = environment;
        temporalType = environment.getElementUtils().getTypeElement("java.time.temporal.Temporal").asType();
        oldDateType = environment.getElementUtils().getTypeElement("java.util.Date").asType();
    }

    boolean build() throws Exception {
        if (builder.beansByInterface.isEmpty()) return false;

        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setIgnoringComments(false);
        docBuilderFactory.setNamespaceAware(true);
        docBuilderFactory.setValidating(false);

        HashMap<String, TypeElement> pkgClasses = new HashMap<>();
        for (String aPackage : builder.packages) {
            PackageElement pkg = environment.getElementUtils().getPackageElement(aPackage);
            for (Element element : pkg.getEnclosedElements()) {
                pkgClasses.put(element.asType().toString(), (TypeElement) element);
            }
        }

        for (DataBeanBuilder bean : builder.beansByInterface.values()) {
            buildMappingFor(pkgClasses.get(bean.interfaceType.toString()), bean);
        }

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
                cf.startBlock("public org.springframework.core.io.Resource[] dDataResources(org.springframework.context.ApplicationContext context) {");
                cf.println("java.util.List<org.springframework.core.io.Resource> r = new java.util.ArrayList<>();");
                for (DataRepositoryBuilder repository : builder.repositories)
                    cf.println("r.add(context.getResource(\"classpath:" +
                            repository.mappingClassName.replaceAll("\\.", "/") +
                            ".xml\"));");
                for (String umf : userMappingFiles) {
                    cf.println("r.add(context.getResource(\"classpath:" +
                            umf.replaceAll("\\.", "/") +
                            ".xml\"));");
                }
                cf.println("return r.toArray(new org.springframework.core.io.Resource[r.size()]);");
                cf.endBlock("}");
                cf.endBlock("}");
            }

        return true;
    }

    class Mapping {
        final List<DataBeanPropertyBuilder> properties = new ArrayList<>();
        final List<DataBeanPropertyBuilder> mappedProperties = new ArrayList<>();
        final boolean manyToOne;

        Mapping(AnnotationMirror annotationMirror, DataBeanBuilder bean) {
            Map<? extends ExecutableElement, ? extends AnnotationValue> map =
                    environment.getElementUtils().getElementValuesWithDefaults(annotationMirror);

            AtomicBoolean hasCollection = new AtomicBoolean(false);
            for (ExecutableElement executableElement : map.keySet()) {
                String mapKey = executableElement.getSimpleName().toString();
                if ("value".equals(mapKey)) {
                    //noinspection unchecked
                    ((List) map.get(executableElement).getValue()).stream()
                            .map(o -> {
                                String n = o.toString();
                                return n.substring(n.lastIndexOf('.') + 1);
                            })
                            .forEach(enumName -> bean.properties.values().stream()
                                    .filter(p -> enumName.equals(p.enumName))
                                    .findAny()
                                    .ifPresent(properties::add));
                } else {
                    //noinspection unchecked
                    ((List) map.get(executableElement).getValue()).stream()
                            .map(Object::toString)
                            .forEach(mapValue -> mappedProperties.add(mappedField(mapKey,
                                    ((String) mapValue), bean, hasCollection)));
                }
            }
            manyToOne = hasCollection.get();
        }

        Mapping(DataBeanPropertyBuilder property, DataBeanBuilder mappedBean) {
            properties.add(property);
            manyToOne = false;
            mappedBean.properties.values().stream()
                    .filter(p -> p.isId).forEach(mappedProperties::add);
        }

        private Mapping(DataBeanPropertyBuilder property, DataBeanPropertyBuilder mappedBeanProperty, boolean manyToOne) {
            properties.add(property);
            mappedProperties.add(mappedBeanProperty);
            this.manyToOne = manyToOne;
        }

        Stream<Mapping> stream() {
            List<Mapping> l = new ArrayList<>();
            if (properties.size() > 0 && mappedProperties.size() > 0)
                for (int i = 0; i < Math.max(properties.size(), mappedProperties.size()); i++) {
                    l.add(new Mapping(
                            properties.get(i < properties.size() ? i : 0),
                            mappedProperties.get(i < mappedProperties.size() ? i : 0),
                            manyToOne
                    ));
                }
            return l.stream();
        }
    }

    private DataBeanPropertyBuilder mappedField(String aParam, String enumName, DataBeanBuilder bean, AtomicBoolean hasCollection) {
        final String shortEnumName = enumName.substring(enumName.lastIndexOf('.') + 1);
        DataBeanPropertyBuilder mappedField = null;
        DataBeanPropertyBuilder mappedBy = bean.properties.values().stream()
                .filter(p -> p.name.equals(aParam)).findAny().orElse(null);
        if (mappedBy != null) {
            DataBeanBuilder mappedBean;
            if (!mappedBy.isCollectionOrMap()) {
                mappedBean = builder.beansByInterface.get(
                        environment.getTypeUtils().erasure(mappedBy.type).toString()
                );
            } else {
                hasCollection.set(true);
                TypeMirror beanType = ((DeclaredType) mappedBy.type).getTypeArguments().get(0);
                mappedBean = builder.beansByInterface.get(
                        environment.getTypeUtils().erasure(beanType).toString()
                );
            }
            mappedField = mappedBean == null ? null : mappedBean.properties.values().stream()
                    .filter(p -> p.enumName.equals(shortEnumName)).findAny().orElse(null);
        }
        return mappedField;
    }

    private void buildMappingFor(TypeElement beanElement, DataBeanBuilder bean) {
        for (Element element : beanElement.getEnclosedElements()) {
            if (element.getKind() == ElementKind.METHOD) {
                ExecutableElement method = (ExecutableElement) element;
                String mappingKey = beanElement.asType().toString() + "." +
                        propertyName4Method(method.getSimpleName().toString());
                Mapping mapping = null;
                for (AnnotationMirror annotationMirror : method.getAnnotationMirrors())
                    if (annotationMirror.getAnnotationType().toString().contains("_Map_")) {
                        mapping = new Mapping(annotationMirror, bean);
                        mappings.put(mappingKey, mapping);

                        /*System.out.println(annotationMirror.getAnnotationType() + " tail:");
                        System.out.println(mappingKey + "\n   " +
                                beanElement.asType().toString() + "." + mapping.property.name +
                                (mapping.manyToOne ? " <- " : " -> ") +
                                mapping.mappedProperty.dataBean.interfaceType + "." +
                                mapping.mappedProperty.name);*/
                        break;
                    }
                if (mapping == null && method.getReturnType() != null) {
                    DataBeanBuilder mappedBean = builder.beansByInterface.get(environment.getTypeUtils().erasure(
                            method.getReturnType()
                    ).toString());
                    if (mappedBean != null) {
                        DataBeanPropertyBuilder property = propertyName4Method(bean, method);
                        mapping = new Mapping(property, mappedBean);
                        mappings.put(mappingKey, mapping);

                        /*System.out.println("method returns " + method.getReturnType() + " tail:");
                        System.out.println(mappingKey + "\n   " +
                                beanElement.asType().toString() + "." + mapping.property.name +
                                (mapping.manyToOne ? " <- " : " -> ") +
                                mapping.mappedProperty.dataBean.interfaceType + "." +
                                mapping.mappedProperty.name);*/
                    }
                }
            }
        }
    }

    private DataBeanPropertyBuilder propertyName4Method(DataBeanBuilder bean, ExecutableElement method) {
        String propName = propertyName4Method(method.getSimpleName().toString());
        return bean.properties.values().stream().filter(p -> p.name.equals(propName)).findAny().orElse(null);
    }

    private String propertyName4Method(String methodSimpleName) {
        if (methodSimpleName.startsWith("get") | methodSimpleName.startsWith("has") || methodSimpleName.startsWith("set"))
            return Character.toLowerCase(methodSimpleName.charAt(3)) + methodSimpleName.substring(4);
        else if (methodSimpleName.startsWith("is"))
            return Character.toLowerCase(methodSimpleName.charAt(2)) + methodSimpleName.substring(3);
        else
            return methodSimpleName;
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
                        m.parameters.size() == methodElement.getParameters().size() &&
                        m.parameters.stream()
                                .map(p -> p.type.toString()).collect(Collectors.joining("|")).equals(
                                methodElement.getParameters().stream()
                                        .map(p -> p.asType().toString()).collect(Collectors.joining("|"))
                        )
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
            for (VariableElement variableElement : methodElement.getParameters()) {
                FilterOption filter = new FilterOption(repository, variableElement);
                filters.add(filter);
            }

            AtomicInteger index = new AtomicInteger();
            CopyOnWriteArrayList<MappedTable> mappedTables = new CopyOnWriteArrayList<>(bean.properties.values().stream()
                    .map(p -> {
                        DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                        return b == null ? null : new MappedTable(0, index.incrementAndGet(), p, b, fetchOptions);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));

            String methodName = method == repository.defaultGetMethod || method == repository.defaultDeleteMethod ?
                    method.methodName :
                    method.methodName + "_" + method.methodIndex;
            switch (method.methodType) {
                case SELECT:
                case GET:
                    if (fetchOptions.resultMap.length() == 0)
                        buildResultMap(mapperRoot, method.repositoryBuilder, methodName, fetchOptions, mappedTables);

                    org.w3c.dom.Element select = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    select.setAttribute("id", methodName);
                    select.setAttribute("parameterType", "HashMap");
                    if (fetchOptions.resultMap.length() == 0)
                        select.setAttribute("resultMap", methodName + "_ResultMap");
                    else {
                        select.setAttribute("resultMap", fetchOptions.resultMap);
                        userMappingFiles.add(fetchOptions.resultMap.substring(0, fetchOptions.resultMap.lastIndexOf('.')));
                    }
                    buildSql(repository, method, bean, fetchOptions, mappedTables, select, filters, false);
                    break;
                case INSERT:
                    org.w3c.dom.Element insert = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("insert"));
                    insert.setAttribute("id", methodName);
                    insert.setAttribute("parameterType", "HashMap");
                    buildSql(repository, method, bean, fetchOptions, mappedTables, insert, filters, false);
                    break;
                case UPDATE:
                    org.w3c.dom.Element update = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("update"));
                    update.setAttribute("id", methodName);
                    update.setAttribute("parameterType", "HashMap");
                    buildSql(repository, method, bean, fetchOptions, mappedTables, update, filters, false);
                    break;
                default: //case DELETE:
                    org.w3c.dom.Element delete = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    delete.setAttribute("id", methodName);
                    delete.setAttribute("parameterType", "HashMap");
                    buildSql(repository, method, bean, fetchOptions, mappedTables, delete, filters, false);
            }
        } else throw new Exception("not found info about method '" + methodElement.getSimpleName() +
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
            switch (method.methodType) {
                case GET:
                case SELECT:
                    sql.append("\nSELECT\n");
                    sql.append(bean.properties.values().stream()
                            .filter(p -> !p.isCollectionOrMap())
                            .filter(fetchOptions::filterIgnored)
                            .map(p -> "  t0." + p.getColumnRef() + " AS " + p.getColumnRef())
                            .collect(Collectors.joining(",\n")));
                    if (fetchOptions.fetchType != DDataFetchType.NO)
                        mappedTables.stream().filter(MappedTable::useInFieldsList).forEach(t ->
                                addManagedBeanToFrom(sql, t, fetchOptions));
                    sql.append("\nFROM ").append(bean.getTableRef()).append(" AS t0\n");
                    break;
                case INSERT:
                    sql.append("\nINSERT INTO ").append(bean.getTableRef()).append(" (");
                    sql.append(bean.properties.values().stream()
                            .filter(p -> !p.isCollectionOrMap())
                            .map(DataBeanPropertyBuilder::getColumnRef)
                            .collect(Collectors.joining(", ")));
                    sql.append(")\n");
                    sql.append("VALUES (\n");
                    sql.append(bean.properties.values().stream()
                            .filter(p -> !p.isCollectionOrMap())
                            .map(p -> buildSqlParameter(bean, p))
                            .collect(Collectors.joining(",\n")));
                    sql.append("\n)\n");
                    break;
                case UPDATE:
                    sql.append("\nUPDATE ").append(bean.getTableRef()).append(" AS t0 SET\n");
                    sql.append(bean.properties.values().stream()
                            .filter(p -> !p.isCollectionOrMap())
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
                case GET:
                case UPDATE:
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
                            addFiltersToSql(domElement, sql, bean, method, mappedTables, filters);
                    } else {
                        addJoins(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()), sql);
                        StringBuilder ssql;
                        if (defaultGet || method == repository.defaultGetMethod) {
                            buildSelect4DefaultGet(domElement, sql);
                            ssql = new StringBuilder("\n");
                        } else ssql = sql;

                        ssql.append("WHERE ");
                        ssql.append(bean.properties.values().stream()
                                .filter(p -> p.isId)
                                .map(p -> "t0." + p.getColumnRef() + " = " + buildSqlParameter(bean, p))
                                .collect(Collectors.joining(" AND ")))
                                .append("\n");

                        domElement.appendChild(doc.createTextNode(ssql.toString()));
                    }
                    break;
                default: //LIST
                    if (filters != null && filters.size() > 0)
                        addFiltersToSql(domElement, sql, bean, method, mappedTables, filters);
                    else {
                        addJoins(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()), sql);
                        domElement.appendChild(doc.createTextNode(sql.toString()));
                    }
            }
        }
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
            ArrayList<FilterOption> filters
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
                        .filter(mb -> mb.mappedFromTableIndex == 1 || filter.property.dataBean != bean)
                        .filter(mb -> mb.property.dataBean == filter.mappedBy.dataBean)
                        //.filter(mb -> mb.mappedByProperty.equals(filter.mappedBy.name))
                        .filter(mb -> mb.property.name.equals(filter.mappedBy.name)
                        ).findAny();
                table.ifPresent(joins::add);
                int tIdx = table.map(mb -> mb.tableIndex).orElse(0);
                MappedTable currentJoin = table.orElse(null);

                switch (filter.option) {
                    case EQUALS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " = #{" + filter.parameter + "}\n"));
                        break;
                    case NOT_EQUALS:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " != #{" + filter.parameter + "}\n"));
                        break;
                    case IS_NULL:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " == TRUE");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " IS NULL\n"));
                        e.setAttribute("test", filter.parameter + " == FALSE");
                        e.appendChild(doc.createTextNode("AND NOT t" + tIdx + "." +
                                filter.property.getColumnRef() + " IS NULL\n"));
                        break;
                    case IN:
                        e = null;
                        //TODO
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
                                filter.property.getColumnRef() + " LIKE #{" + filter.parameter + "}\n"));
                        break;
                    case ILIKE:
                        e = doc.createElement("if");
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                filter.property.getColumnRef() + " ILIKE #{" + filter.parameter + "}\n"));
                    default:
                        e = null;
                }

                if (e != null)
                    if (currentJoin != null && !currentJoin.useInFieldsList) {
                        IfExists ifExists = whereExists.get(currentJoin.mappedBean.getTableRef());
                        if (ifExists == null) {
                            org.w3c.dom.Element existsElt = doc.createElement("trim");
                            existsElt.setAttribute("prefix", "AND EXISTS (SELECT * FROM " +
                                    currentJoin.mappedBean.getTableRef() + " AS t" + tIdx + " WHERE ");
                            existsElt.setAttribute("prefixOverrides", "AND ");
                            existsElt.setAttribute("suffix", ")\n");
                            Mapping joinMap = mappings.get(currentJoin.property.dataBean.interfaceType.toString() +
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
                            whereExists.put(currentJoin.mappedBean.getTableRef(), ifExists);
                        } else {
                            ifExists.parameters.add(filter.parameter);
                            ifExists.element.appendChild(e);
                        }
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
        addJoins(joins, sql);

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

        filters.stream().filter(f -> f.option == DDataFilterOption.LIMIT).findAny().ifPresent(limit -> {
            org.w3c.dom.Element limitElt = (org.w3c.dom.Element)
                    domElement.appendChild(doc.createElement("if"));
            limitElt.setAttribute("test", limit.parameter + " != null");
            limitElt.appendChild(doc.createTextNode("LIMIT #{" + limit.parameter + "}\n"));
            filters.stream().filter(f -> f.option == DDataFilterOption.START).findAny().ifPresent(offset -> {
                org.w3c.dom.Element offsetElt = (org.w3c.dom.Element)
                        limitElt.appendChild(doc.createElement("if"));
                offsetElt.setAttribute("test", offset.parameter + " != null && " + offset.parameter + " != 0");
                offsetElt.appendChild(doc.createTextNode("OFFSET #{" + offset.parameter + "}\n"));
            });
        });
    }

    private void addJoins(Collection<MappedTable> joins, StringBuilder sql) {
        joins.stream()
                .filter(t -> t.useInFieldsList)
                .sorted(Comparator.comparingInt(t -> t.tableIndex))
                .forEach(join -> {
                    Mapping joinMap = mappings.get(join.property.dataBean.interfaceType.toString() + "." + join.property.name);
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
        DDataMapBuilder.Mapping mapping = mappings.get(dataBean.interfaceType.toString() + "." + beanProperty.name);
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
        if (//not work environment.getTypeUtils().isSubtype(type, temporalType) ||
                environment.getTypeUtils().directSupertypes(type).stream()
                        .anyMatch(c -> c.toString().equals(temporalType.toString())) ||
                        environment.getTypeUtils().isSubtype(type, oldDateType)
                ) return ", jdbcType=TIMESTAMP";

        if (String.class.getCanonicalName().equals(s))
            return ", jdbcType=VARCHAR";

        if ("int".equals(s) ||
                "long".equals(s) ||
                "double".equals(s) ||
                "short".equals(s) ||
                java.lang.Integer.class.getCanonicalName().equals(s) ||
                java.lang.Long.class.getCanonicalName().equals(s) ||
                java.lang.Double.class.getCanonicalName().equals(s) ||
                java.lang.Short.class.getCanonicalName().equals(s) ||
                java.math.BigInteger.class.getCanonicalName().equals(s) ||
                java.math.BigDecimal.class.getCanonicalName().equals(s)
                ) return ", jdbcType=NUMERIC";
        return "";
    }

    private void addManagedBeanToFrom(StringBuilder sql, MappedTable mappedTable, FetchOptions fetchOptions) {
        String r = mappedTable.mappedBean.properties.values().stream()
                .filter(fetchOptions::filter4FieldsList)
                .map(p -> "  t" + mappedTable.tableIndex + "." + p.getColumnRef() +
                        " AS t" + mappedTable.tableIndex + "_" + p.columnName)
                .collect(Collectors.joining(",\n"));
        if (r.length() > 0) sql.append(",\n").append(r);
    }

    private void buildResultMap(
            org.w3c.dom.Element mapperRoot,
            DataRepositoryBuilder repository, String methodName,
            FetchOptions fetchOptions,
            CopyOnWriteArrayList<MappedTable> mappedTables) {
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
                        addManagedBeanToResultMap(map, b, fetchOptions, repository, mappedTables, fetchOptions.eagerTrunkLevel));
        mappedTables.stream()
                .filter(b -> fetchOptions.filter4ResultMap(b.property))
                .filter(b -> b.property.isCollectionOrMap())
                .forEach(b ->
                        addManagedBeanToResultMap(map, b, fetchOptions, repository, mappedTables, fetchOptions.eagerTrunkLevel));
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
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            element.appendChild(doc.createElement("result"));
                    boolean isBean = builder.beansByInterface.containsKey(p.type.toString());
                    id.setAttribute("property", p.name + (isBean ? "_foreignKey" : ""));
                    id.setAttribute("column", prefix + p.columnName);
                });
    }

    private void addManagedBeanToResultMap(
            org.w3c.dom.Element map,
            MappedTable mappedTable,
            FetchOptions fetchOptions,
            DataRepositoryBuilder repository,
            List<MappedTable> mappedTables,
            int trunkLevel
    ) {
        Document doc = map.getOwnerDocument();
        org.w3c.dom.Element managed = (org.w3c.dom.Element) map.appendChild(doc.createElement(
                mappedTable.property.isCollectionOrMap() ? "collection" : "association"));
        managed.setAttribute("property", mappedTable.property.name);
        managed.setAttribute("javaType", mappedTable.property.isCollection ? "ArrayList" : (
                mappedTable.property.isMap ? "HashMap" : mappedTable.mappedBean.getImplementationName())
        );
        Mapping mapping = mappings.get(mappedTable.property.dataBean.interfaceType + "." + mappedTable.property.name);
        boolean lazy;
        if (mappedTable.property.isCollectionOrMap()) {
            managed.setAttribute("ofType", mappedTable.property.dataBean.getImplementationName());
            lazy = fetchOptions.fetchType != DDataFetchType.EAGER;
        } else {
            lazy = fetchOptions.fetchType == DDataFetchType.LAZY;
        }

        if (lazy || trunkLevel == -2) {
            managed.setAttribute("column", mapping.properties.stream()
                    .map(p -> p.columnName)
                    .collect(Collectors.joining(",")));
            managed.setAttribute("foreignColumn", mapping.mappedProperties.stream()
                    .map(p -> p.columnName)
                    .collect(Collectors.joining(",")));

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
                                    m.properties.get(0).type : mapping.mappedProperties.get(0).type;
                            return "t0." +
                                    m.mappedProperties.get(0).getColumnRef() + " = " +
                                    "#{" + m.properties.get(0).columnName +
                                    ", javaType=" + (propType.getKind().isPrimitive() ?
                                    environment.getTypeUtils().boxedClass((PrimitiveType) propType) : propType) +
                                    jdbcTypeFor(propType, environment) + "}";
                        }).collect(Collectors.joining(" AND ")) +
                        "\n"));

                repository.lazyLoads.put(lazyLoadSelectId, ll);
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
                                        fetchOptions, repository, mappedTables, trunkLevel)
                        );
                mappedBeans.stream()
                        .filter(DataBeanPropertyBuilder::isCollectionOrMap)
                        .forEach(mappedBean ->
                                addManaged2BeanToResultMap(managed, mappedTable, mappedBean,
                                        fetchOptions, repository, mappedTables, trunkLevel)
                        );
            }
        }
    }

    private void addManaged2BeanToResultMap(org.w3c.dom.Element managed, MappedTable mappedTable, DataBeanPropertyBuilder mappedBean, FetchOptions fetchOptions, DataRepositoryBuilder repository, List<MappedTable> mappedTables, int trunkLevel) {
        DataBeanBuilder mapped2Bean = builder.beansByInterface.get(mappedBean.mappedType.toString());
        if (mapped2Bean != null) {
            MappedTable mapped2Table = new MappedTable(mappedTable.tableIndex, mappedTables.size() + 1,
                    mappedBean, mapped2Bean, fetchOptions);
            mappedTables.add(mapped2Table);

            addManagedBeanToResultMap(managed, mapped2Table, fetchOptions, repository, mappedTables,
                    trunkLevel == -1 ? -2 : trunkLevel - 1);
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
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, false);
    }

    private void createSimpleUpdate(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("update"));
        select.setAttribute("id", "update");

        select.setAttribute("parameterType", bean.getImplementationName());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.UPDATE);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, false);
    }

    private void createSimpleInsert(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("insert"));
        select.setAttribute("id", "insert");

        select.setAttribute("parameterType", bean.getImplementationName());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.INSERT);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, false);
    }

    private void createSimpleGet(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        AtomicInteger index = new AtomicInteger();
        CopyOnWriteArrayList<MappedTable> mappedTables = new CopyOnWriteArrayList<>(bean.properties.values().stream()
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                    return b == null ? null : new MappedTable(0, index.incrementAndGet(), p, b, fetchOptions);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        Document doc = mapperRoot.getOwnerDocument();

        buildResultMap(mapperRoot, repository, "get", fetchOptions, mappedTables);

        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("select"));
        select.setAttribute("id", "get");
        select.setAttribute("resultMap", "get_ResultMap");

        select.setAttribute("parameterType", bean.keyType);
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.GET);
        buildSql(repository, method, bean, fetchOptions, mappedTables, select, null, true);
    }

    private class FilterOption {
        final DDataFilterOption option;
        final DataBeanPropertyBuilder property;
        final String parameter;
        final DataBeanPropertyBuilder mappedBy;

        FilterOption(DataRepositoryBuilder repository, VariableElement variableElement) {
            DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName.toString());
            Optional<? extends AnnotationMirror> filterOpt = variableElement.getAnnotationMirrors().stream()
                    .filter(a -> a.toString().indexOf("_Filter_") > 0)
                    .findAny();

            parameter = variableElement.getSimpleName().toString();

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
                    DataBeanBuilder mappedBean = builder.beansByInterface.get(property.mappedType.toString());
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
                option = DDataFilterOption.EQUALS;
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
                    .orElse(-1);
        }

        FetchOptions(int trunkLevel) {
            fetchType = DDataFetchType.COLLECTIONS_ARE_LAZY;
            sqlSelect = "";
            resultMap = "";
            ignore = Collections.emptyList();
            eagerTrunkLevel = trunkLevel;
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

        MappedTable(int fromTableIndex, int tableIndex, DataBeanPropertyBuilder p, DataBeanBuilder b, FetchOptions fetchOptions) {
            this.tableIndex = tableIndex;
            this.mappedFromTableIndex = fromTableIndex;
            this.property = p;
            this.mappedBean = b;
            this.useInFieldsList = fetchOptions.filter4FieldsList(p);
        }

        boolean useInFieldsList() {
            return useInFieldsList;
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
