package org.docero.data.processor;

import org.docero.data.DDataFetchType;
import org.docero.data.DDataFilterOption;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.NodeList;

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

class DDataMapBuilder {
    private final DDataBuilder builder;
    private final ProcessingEnvironment environment;
    private final HashMap<String, Mapping> mappings = new HashMap<>();
    private final TypeMirror temporalType;
    private final TypeMirror oldDateType;

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
        return true;
    }

    class Mapping {
        final DataBeanPropertyBuilder property;
        final DataBeanPropertyBuilder mappedProperty;
        final boolean manyToOne;

        Mapping(AnnotationMirror annotationMirror, DataBeanBuilder bean) {
            Map<? extends ExecutableElement, ? extends AnnotationValue> map =
                    environment.getElementUtils().getElementValuesWithDefaults(annotationMirror);

            DataBeanPropertyBuilder localField = null;
            DataBeanPropertyBuilder mappedField = null;
            AtomicBoolean hasCollection = new AtomicBoolean(false);
            for (ExecutableElement executableElement : map.keySet()) {
                String mapKey = executableElement.getSimpleName().toString();
                String enumName;
                if ("value".equals(mapKey)) {
                    enumName = map.get(executableElement).getValue().toString();
                    localField = bean.properties.values().stream()
                            .filter(p -> p.enumName.equals(enumName)).findAny().orElse(null);
                } else {
                    Object mapValue = map.get(executableElement).getValue();
                    if (mapValue instanceof List) {
                        if (((List) mapValue).size() > 0) {
                            mappedField = mappedField(mapKey,
                                    ((List) mapValue).get(0).toString(), bean, hasCollection);
                        }
                    } else {
                        mappedField = mappedField(mapKey, mapValue.toString(), bean, hasCollection);
                    }
                }
            }
            property = localField;
            mappedProperty = mappedField;
            manyToOne = hasCollection.get();
        }

        Mapping(DataBeanPropertyBuilder property, DataBeanBuilder mappedBean) {
            this.property = property;
            manyToOne = false;
            mappedProperty = mappedBean.properties.values().stream()
                    .filter(p -> p.isId).findFirst().orElse(null);
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
                    buildResultMap(mapperRoot, method.repositoryBuilder, methodName, fetchOptions, mappedTables);

                    org.w3c.dom.Element select = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    select.setAttribute("id", methodName);
                    select.setAttribute("parameterType", "HashMap");
                    select.setAttribute("resultMap", methodName + "_ResultMap");
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
        switch (method.methodType) {
            case GET:
            case SELECT:
                sql.append("\nSELECT\n");
                sql.append(bean.properties.values().stream()
                        .filter(p -> !p.isCollectionOrMap())
                        .filter(fetchOptions::filterIgnored)
                        .map(p -> "  t0." + p.columnName + " AS " + p.columnName)
                        .collect(Collectors.joining(",\n")));
                if (fetchOptions.fetchType != DDataFetchType.NO)
                    mappedTables.stream().filter(MappedTable::useInFieldsList).forEach(t ->
                            addManagedBeanToFrom(sql, t, fetchOptions));
                sql.append("\nFROM ").append(bean.table).append(" AS t0\n");
                break;
            case INSERT:
                sql.append("\nINSERT INTO ").append(bean.table).append(" (");
                sql.append(bean.properties.values().stream()
                        .filter(p -> !p.isCollectionOrMap())
                        .map(p -> p.columnName)
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
                sql.append("\nUPDATE ").append(bean.table).append(" SET\n");
                sql.append(bean.properties.values().stream()
                        .filter(p -> !p.isCollectionOrMap())
                        .map(p -> p.columnName + " = " + buildSqlParameter(bean, p))
                        .collect(Collectors.joining(",\n")))
                        .append("\n");
                break;
            case DELETE:
                sql.append("\nDELETE FROM ").append(bean.table).append(' ');
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
                                        .map(f -> f.property.columnName + " = " + buildSqlParameter(bean, f.property))
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
                            .map(p -> p.columnName + " = " + buildSqlParameter(bean, p))
                            .collect(Collectors.joining(" AND ")))
                            .append("\n");

                    domElement.appendChild(doc.createTextNode(ssql.toString()));
                }
                break;
            default: //LIST
                /*boolean limitedList = filters.stream().anyMatch(f -> f.option == DDataFilterOption.LIMIT);
                if (limitedList)
                    sql.insert(0, "\nSELECT * FROM (");*/

                if (filters != null && filters.size() > 0)
                    addFiltersToSql(domElement, sql, bean, method, mappedTables, filters);
                else {
                    addJoins(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()), sql);
                    domElement.appendChild(doc.createTextNode(sql.toString()));
                }
                /*if (limitedList) {
                    StringBuilder limitSql = new StringBuilder();
                    filters.stream().filter(f -> f.option == DDataFilterOption.LIMIT).findAny()
                            .ifPresent(f -> limitSql.append("\n) LIMIT #{").append(f.parameter).append("}"));
                    filters.stream().filter(f -> f.option == DDataFilterOption.START).findAny()
                            .ifPresent(f -> limitSql.append(" OFFSET #{").append(f.parameter).append("}"));
                    limitSql.append("\n");
                    domElement.appendChild(doc.createTextNode(limitSql.toString()));
                }*/
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

        org.w3c.dom.Element where = doc.createElement("trim");
        where.setAttribute("prefix", "WHERE");
        where.setAttribute("prefixOverrides", "AND ");

        HashSet<MappedTable> joins = new HashSet<>();
        joins.addAll(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()));

        org.w3c.dom.Element e;
        for (FilterOption filter : filters)
            if (filter.option != null && filter.property != null) {
                int tIdx = 0;
                Optional<MappedTable> table = mappedTables.stream()
                        .filter(mb -> mb.mappedFromTableIndex == 1 || filter.property.dataBean != bean)
                        .filter(mb -> mb.property.dataBean == filter.mappedBy.dataBean)
                        //.filter(mb -> mb.mappedByProperty.equals(filter.mappedBy.name))
                        .filter(mb -> mb.property.name.equals(filter.mappedBy.name)
                        ).findAny();
                table.ifPresent(joins::add);
                tIdx = table.map(mb -> mb.tableIndex).orElse(0);

                switch (filter.option) {
                    case EQUALS:
                        e = (org.w3c.dom.Element) where.appendChild(doc.createElement("if"));
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("t" + tIdx + "." +
                                filter.property.columnName + " = #{" + filter.parameter + "}"));
                        break;
                    case NOT_EQUALS:
                        e = (org.w3c.dom.Element) where.appendChild(doc.createElement("if"));
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("t" + tIdx + "." +
                                filter.property.columnName + " != #{" + filter.parameter + "}"));
                        break;
                    case IS_NULL:
                        e = (org.w3c.dom.Element) where.appendChild(doc.createElement("if"));
                        e.setAttribute("test", filter.parameter + " == TRUE");
                        e.appendChild(doc.createTextNode("t" + tIdx + "." +
                                filter.property.columnName + " IS NULL}"));
                        e.setAttribute("test", filter.parameter + " == FALSE");
                        e.appendChild(doc.createTextNode("NOT t" + tIdx + "." +
                                filter.property.columnName + " IS NULL}"));
                        break;
                    case IN:
                        //TODO
                        break;
                    case LIKE:
                    case LIKE_HAS:
                    case LIKE_STARTS:
                    case LIKE_ENDS:
                    case LIKE_ALL_STARTS:
                    case LIKE_ALL_HAS:
                        e = (org.w3c.dom.Element) where.appendChild(doc.createElement("if"));
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("t" + tIdx + "." +
                                filter.property.columnName + " LIKE #{" + filter.parameter + "}"));
                        break;
                    case ILIKE:
                        e = (org.w3c.dom.Element) where.appendChild(doc.createElement("if"));
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("t" + tIdx + "." +
                                filter.property.columnName + " ILIKE #{" + filter.parameter + "}"));
                }
            }

        addJoins(joins, sql);

        if (method.methodType == DDataMethodBuilder.MType.SELECT) {
            domElement.appendChild(doc.createTextNode(sql.toString()));
            if (filters.size() > 0) domElement.appendChild(where);
        } else {
            sql.append("WHERE\n");
            domElement.appendChild(doc.createTextNode(sql.toString()));
            NodeList nl = where.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) domElement.appendChild(nl.item(i));
        }
    }

    private void addJoins(Collection<MappedTable> joins, StringBuilder sql) {
        for (MappedTable join : joins) {
            Mapping joinMap = mappings.get(join.property.dataBean.interfaceType.toString() + "." + join.property.name);
                    /*System.out.println("method returns " + method.getReturnType() + " tail:");
                        System.out.println(mappingKey + "\n   " +
                                beanElement.asType().toString() + "." + mapping.property.name +
                                (mapping.manyToOne ? " <- " : " -> ") +
                                mapping.mappedProperty.dataBean.interfaceType + "." +
                                mapping.mappedProperty.name);*/
            sql.append("LEFT JOIN ")
                    .append(join.mappedBean.table)
                    .append(" AS t").append(join.tableIndex)
                    .append(" ON (t").append(join.mappedFromTableIndex)
                    .append(".").append(joinMap.property.columnName)
                    .append(" = t").append(join.tableIndex)
                    .append(".").append(joinMap.mappedProperty.columnName).append(")\n");
        }
    }

    /**
     * not applicable for collection types
     *
     * @return string like #{name,javaType=...,jdbcType=...}
     */
    private String buildSqlParameter(DataBeanBuilder dataBean, DataBeanPropertyBuilder beanProperty) {
        DDataMapBuilder.Mapping mapping = mappings.get(dataBean.interfaceType.toString() + "." + beanProperty.name);
        if (mapping != null) {
            return "#{" + beanProperty.name + "_foreignKey, javaType=" + (mapping.mappedProperty.type.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) mapping.mappedProperty.type) :
                    mapping.mappedProperty.type
            ) + jdbcTypeFor(mapping.mappedProperty.type, environment) + "}";
        } else
            return "#{" + beanProperty.name + ", javaType=" + (beanProperty.type.getKind().isPrimitive() ?
                    environment.getTypeUtils().boxedClass((PrimitiveType) beanProperty.type) :
                    beanProperty.type
            ) + jdbcTypeFor(beanProperty.type, environment) + "}";
    }

    private String jdbcTypeFor(TypeMirror type, ProcessingEnvironment environment) {
        String s = type.toString();
        if (environment.getTypeUtils().isSubtype(type, temporalType) ||
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
                .map(p -> "  t" + mappedTable.tableIndex + "." + p.columnName +
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

        mappedTables.stream().filter(b -> fetchOptions.filter4ResultMap(b.property)).forEach(b ->
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
                .filter(p -> !(p.isId || p.isCollectionOrMap()))
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
                mappedTable.property.isMap ? "HashMap" : mappedTable.property.dataBean.getImplementationName())
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
            TypeMirror propType = mapping.manyToOne ?
                    mapping.property.type : mapping.mappedProperty.type;
            managed.setAttribute("column", mapping.manyToOne ?
                    mapping.mappedProperty.columnName : mapping.property.columnName);
            managed.setAttribute("foreignColumn", mapping.manyToOne ?
                    mapping.property.columnName : mapping.mappedProperty.columnName);

            managed.setAttribute("fetchType", "lazy");
            String lazyLoadSelectId = "lazy_load_" + mapping.mappedProperty.dataBean.name +
                    "_" + mapping.mappedProperty.columnName;
            managed.setAttribute("select", lazyLoadSelectId);
            if (!repository.lazyLoads.containsKey(lazyLoadSelectId)) {
                DataRepositoryBuilder beanRep =
                        builder.repositoriesByBean.get(mapping.mappedProperty.dataBean.interfaceType.toString());
                org.w3c.dom.Element ll = doc.createElement("select");
                ll.setAttribute("id", lazyLoadSelectId);
                ll.setAttribute("resultMap", repository == beanRep ?
                        "get_ResultMap" : beanRep.mappingClassName + ".get_ResultMap");
                org.w3c.dom.Element il = (org.w3c.dom.Element)
                        ll.appendChild(doc.createElement("include"));
                il.setAttribute("refid", repository == beanRep ?
                        "get_select" : beanRep.mappingClassName + ".get_select");
                ll.appendChild(doc.createTextNode("\nWHERE t0." +
                        mapping.mappedProperty.columnName + " = " +
                        //buildSqlParameter(mapping.mappedProperty.dataBean, mapping.mappedProperty) +
                        "#{" + mapping.property.columnName + ", javaType=" + (propType.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) propType) : propType
                ) + jdbcTypeFor(propType, environment) + "}" +
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

                for (DataBeanPropertyBuilder mappedBean : mappedBeans) {
                    DataBeanBuilder mapped2Bean = builder.beansByInterface.get(mappedBean.mappedType.toString());
                    if (mapped2Bean != null) {
                        MappedTable mapped2Table = new MappedTable(mappedTable.tableIndex, mappedTables.size() + 1,
                                mappedBean, mapped2Bean, fetchOptions);
                        mappedTables.add(mapped2Table);

                        addManagedBeanToResultMap(managed, mapped2Table, fetchOptions, repository, mappedTables,
                                trunkLevel == -1 ? -2 : trunkLevel - 1);
                    }
                }
            }
        }
    }

    private void createSimpleDelete(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("delete"));
        select.setAttribute("id", "delete");

        select.setAttribute("parameterType", bean.keyType);
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.DELETE, environment);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, false);
    }

    private void createSimpleUpdate(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("update"));
        select.setAttribute("id", "update");

        select.setAttribute("parameterType", bean.interfaceType.toString());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.UPDATE, environment);
        buildSql(repository, method, bean, fetchOptions, Collections.emptyList(), select, null, false);
    }

    private void createSimpleInsert(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository, FetchOptions fetchOptions) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("insert"));
        select.setAttribute("id", "insert");

        select.setAttribute("parameterType", bean.interfaceType.toString());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.INSERT, environment);
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
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.GET, environment);
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
        final String sqlFrom;
        final String resultMap;
        final String alias;
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

            sqlFrom = fetchProps.keySet().stream()
                    .filter(k -> "from".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue().toString())
                    .orElse("");

            resultMap = fetchProps.keySet().stream()
                    .filter(k -> "resultMap".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue().toString())
                    .orElse("");

            alias = fetchProps.keySet().stream()
                    .filter(k -> "alias".equals(k.getSimpleName().toString()))
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
            sqlFrom = "";
            resultMap = "";
            alias = "";
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
}
