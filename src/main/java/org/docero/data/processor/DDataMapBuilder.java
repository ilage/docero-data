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
            if (repositoryElement == null) {
                repositoryNamespace = repository.forInterfaceName();
                createSimpleGet(mapperRoot, repository);
                createSimpleInsert(mapperRoot, repository);
                createSimpleUpdate(mapperRoot, repository);
                createSimpleDelete(mapperRoot, repository);
            } else {
                repositoryNamespace = repository.repositoryInterface.toString();

                if (!repository.hasGet) createSimpleGet(mapperRoot, repository);
                if (!repository.hasInsert) createSimpleInsert(mapperRoot, repository);
                if (!repository.hasUpdate) createSimpleUpdate(mapperRoot, repository);
                if (!repository.hasDelete) createSimpleDelete(mapperRoot, repository);

                //System.out.println(repository.repositoryInterface + ":" + repositoryElement.getEnclosedElements());
                for (Element methodElement : repositoryElement.getEnclosedElements())
                    createDefinedMethod(mapperRoot, (ExecutableElement) methodElement, repository);
            }
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
                    .orElse(null);

            ArrayList<FilterOption> filters = new ArrayList<>();
            for (VariableElement variableElement : methodElement.getParameters()) {
                FilterOption filter = new FilterOption(repository, methodElement, variableElement);
                filters.add(filter);
            }

            AtomicInteger index = new AtomicInteger();
            List<MappedTable> mappedBeans = bean.properties.values().stream()
                    .map(p -> {
                        DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                        return b == null ? null : new MappedTable(index.incrementAndGet(), p, b, fetchOptions);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            switch (method.methodType) {
                case SELECT:
                case GET:
                    buildResultMap(mapperRoot, method.repositoryBuilder,
                            method.methodName + "_" + method.methodIndex,
                            fetchOptions, mappedBeans);

                    org.w3c.dom.Element select = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    select.setAttribute("id", method.methodName + "_" + method.methodIndex);
                    select.setAttribute("parameterType", "HashMap");
                    select.setAttribute("resultMap", method.methodName + "_" + method.methodIndex + "_ResultMap");
                    buildSql(method, bean, fetchOptions, mappedBeans, select, filters);
                    break;
                case INSERT:
                    org.w3c.dom.Element insert = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("insert"));
                    insert.setAttribute("id", method.methodName + "_" + method.methodIndex);
                    insert.setAttribute("parameterType", "HashMap");
                    buildSql(method, bean, fetchOptions, mappedBeans, insert, filters);
                    break;
                case UPDATE:
                    org.w3c.dom.Element update = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("update"));
                    update.setAttribute("id", method.methodName + "_" + method.methodIndex);
                    update.setAttribute("parameterType", "HashMap");
                    buildSql(method, bean, fetchOptions, mappedBeans, update, filters);
                    break;
                default: //case DELETE:
                    org.w3c.dom.Element delete = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));
                    delete.setAttribute("id", method.methodName + "_" + method.methodIndex);
                    delete.setAttribute("parameterType", "HashMap");
                    buildSql(method, bean, fetchOptions, mappedBeans, delete, filters);
            }
        } else throw new Exception("not found info about method '" + methodElement.getSimpleName() +
                " of " + repository.repositoryInterface);
    }

    private void buildSql(
            DDataMethodBuilder method,
            DataBeanBuilder bean,
            FetchOptions fetchOptions, List<MappedTable> mappedBeans,
            org.w3c.dom.Element domElement,
            ArrayList<FilterOption> filters
    ) {
        Document doc = domElement.getOwnerDocument();

        StringBuilder sql = new StringBuilder();
        switch (method.methodType) {
            case GET:
            case SELECT:
                sql.append("\nSELECT\n");
                sql.append(bean.properties.values().stream()
                        .filter(p -> !p.isCollectionOrMap())
                        .filter(p -> fetchOptions == null || fetchOptions.filterBasic(p))
                        .map(p -> "  t0." + p.columnName + " AS _" + p.columnName)
                        .collect(Collectors.joining(",\n")));
                if (fetchOptions == null || fetchOptions.fetchType != DDataFetchType.NO)
                    mappedBeans.stream().filter(MappedTable::useInFieldsList).forEach(b ->
                            addManagedBeanToFrom(sql, b, fetchOptions));
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
                    addFiltersToSql(domElement, sql, method, mappedBeans, filters);
                } else {
                    addJoins(mappedBeans.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()), sql);
                    sql.append("WHERE ");
                    sql.append(bean.properties.values().stream()
                            .filter(p -> p.isId)
                            .map(p -> p.columnName + " = " + buildSqlParameter(bean, p))
                            .collect(Collectors.joining(" AND ")))
                            .append("\n");

                    domElement.appendChild(doc.createTextNode(sql.toString()));
                }
                break;
            default: //LIST
                /*boolean limitedList = filters.stream().anyMatch(f -> f.option == DDataFilterOption.LIMIT);
                if (limitedList)
                    sql.insert(0, "\nSELECT * FROM (");*/

                if (filters != null && filters.size() > 0)
                    addFiltersToSql(domElement, sql, method, mappedBeans, filters);
                else {
                    addJoins(mappedBeans.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()), sql);
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

    private void addFiltersToSql(
            org.w3c.dom.Element domElement,
            StringBuilder sql, DDataMethodBuilder method,
            List<MappedTable> mappedBeans,
            ArrayList<FilterOption> filters
    ) {
        Document doc = domElement.getOwnerDocument();

        org.w3c.dom.Element where = doc.createElement("trim");
        where.setAttribute("prefix", "WHERE");
        where.setAttribute("prefixOverrides", "AND ");

        HashSet<MappedTable> joins = new HashSet<>();
        joins.addAll(mappedBeans.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()));

        org.w3c.dom.Element e;
        for (FilterOption filter : filters)
            if (filter.option != null && filter.property != null) {
                Optional<MappedTable> table = mappedBeans.stream()
                        .filter(mb -> mb.property.isCollectionOrMap() ?
                                mb.property.name.equals(filter.property.name) :
                                mb.property.columnName.equals(filter.property.columnName))
                        .findAny();
                table.ifPresent(joins::add);
                int tIdx = table.map(mb -> mb.tableIndex).orElse(0);

                switch (filter.option) {
                    case EQUALS:
                        e = (org.w3c.dom.Element) where.appendChild(doc.createElement("if"));
                        e.setAttribute("test", filter.parameter + " != null");
                        e.appendChild(doc.createTextNode("t" + tIdx + "." +
                                filter.property.columnName + " = #{" + filter.parameter + "}"));
                        break;
                    case NOT_EQUALS:
                        //TODO
                        break;
                    case IS_NULL:
                        //TODO
                        break;
                    case NOT_IS_NULL:
                        //TODO
                        break;
                    case IN:
                        //TODO
                        break;
                    case LIKE:
                        //TODO
                        break;
                    case LIKE_HAS:
                        //TODO
                        break;
                    case LIKE_STARTS:
                        //TODO
                        break;
                    case LIKE_ENDS:
                        //TODO
                        break;
                    case LIKE_ALL_STARTS:
                        //TODO
                        break;
                    case LIKE_ALL_HAS:
                        //TODO
                        break;
                    case ILIKE:
                        //TODO
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
                    .append(" ON (t0.").append(joinMap.property.columnName)
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
                .filter(p -> filter4FieldsList(p, fetchOptions))
                .map(p -> "  t" + mappedTable.tableIndex + "." + p.columnName +
                        " AS " + mappedTable.property.name + "_" + p.columnName)
                .collect(Collectors.joining(",\n"));
        if (r.length() > 0) sql.append(",\n").append(r);
    }

    private void buildResultMap(
            org.w3c.dom.Element mapperRoot,
            DataRepositoryBuilder repository, String methodName,
            FetchOptions fetchOptions,
            List<MappedTable> mappedBeans) {
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element map = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("resultMap"));
        map.setAttribute("id", methodName + "_ResultMap");
        map.setAttribute("type", repository.beanImplementation);

        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());

        bean.properties.values().stream()
                .filter(p -> p.isId)
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            map.appendChild(doc.createElement("id"));
                    id.setAttribute("property", p.name);
                    id.setAttribute("column", "_" + p.columnName);
                });
        /*org.w3c.dom.Element c = (org.w3c.dom.Element) map.appendChild(doc.createElement("ignore"));
        if (fetchOptions!=null) c.appendChild(doc.createTextNode(fetchOptions.ignore.stream()
                .map(f->f.enumName)
                .collect(Collectors.joining(","))));*/

        bean.properties.values().stream()
                .filter(p -> !(p.isId || p.isCollectionOrMap()))
                .filter(p -> fetchOptions == null || fetchOptions.filterBasic(p))
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            map.appendChild(doc.createElement("result"));
                    boolean isBean = builder.beansByInterface.containsKey(p.type.toString());
                    id.setAttribute("property", p.name + (isBean ? "_foreignKey" : ""));
                    id.setAttribute("column", "_" + p.columnName);
                });
        mappedBeans.stream().filter(b -> filter4ResultMap(b.property, fetchOptions)).forEach(b ->
                addManagedBeanToResultMap(map, b, fetchOptions));
    }

    private void addManagedBeanToResultMap(org.w3c.dom.Element map, MappedTable mappedTable, FetchOptions fetchOptions) {
        Document doc = map.getOwnerDocument();
        org.w3c.dom.Element managed = (org.w3c.dom.Element) map.appendChild(doc.createElement(
                mappedTable.property.isCollectionOrMap() ? "collection" : "association"));
        managed.setAttribute("property", mappedTable.property.name);
        managed.setAttribute("javaType", mappedTable.property.isCollection ? "ArrayList" : (
                mappedTable.property.isMap ? "HashMap" :
                        environment.getTypeUtils().erasure(mappedTable.property.type).toString())
        );
        if (mappedTable.property.isCollectionOrMap())
            managed.setAttribute("ofType", mappedTable.property.mappedType.toString());

        mappedTable.mappedBean.properties.values().stream()
                .filter(p -> p.isId)
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            managed.appendChild(doc.createElement("id"));
                    id.setAttribute("property", p.name);
                    id.setAttribute("column", mappedTable.property.name + "_" + p.columnName);
                });

        mappedTable.mappedBean.properties.values().stream()
                .filter(p -> !p.isId)
                .filter(p -> filter4ResultMap(p, fetchOptions))
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            managed.appendChild(doc.createElement("result"));
                    boolean isBean = builder.beansByInterface.containsKey(p.type.toString());
                    id.setAttribute("property", p.name + (isBean ? "_foreignKey" : ""));
                    id.setAttribute("column", mappedTable.property.name + "_" + p.columnName);
                });
    }

    private void createSimpleDelete(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("delete"));
        select.setAttribute("id", "delete");

        select.setAttribute("parameterType", bean.keyType);
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.DELETE, environment);
        buildSql(method, bean, null, Collections.emptyList(), select, null);
    }

    private void createSimpleUpdate(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("update"));
        select.setAttribute("id", "update");

        select.setAttribute("parameterType", bean.interfaceType.toString());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.UPDATE, environment);
        buildSql(method, bean, null, Collections.emptyList(), select, null);
    }

    private void createSimpleInsert(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("insert"));
        select.setAttribute("id", "insert");

        select.setAttribute("parameterType", bean.interfaceType.toString());
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.INSERT, environment);
        buildSql(method, bean, null, Collections.emptyList(), select, null);
    }

    private void createSimpleGet(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        AtomicInteger index = new AtomicInteger();
        List<MappedTable> mappedBeans = bean.properties.values().stream()
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                    return b == null ? null : new MappedTable(index.incrementAndGet(), p, b, null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        Document doc = mapperRoot.getOwnerDocument();

        buildResultMap(mapperRoot, repository, "get", null, mappedBeans);

        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("select"));
        select.setAttribute("id", "get");
        select.setAttribute("resultMap", "get_ResultMap");

        select.setAttribute("parameterType", bean.keyType);
        DDataMethodBuilder method = new DDataMethodBuilder(repository, DDataMethodBuilder.MType.GET, environment);
        buildSql(method, bean, null, mappedBeans, select, null);
    }

    private class FilterOption {
        final DDataFilterOption option;
        final DataBeanPropertyBuilder property;
        final String parameter;

        FilterOption(DataRepositoryBuilder repository, ExecutableElement methodElement, VariableElement variableElement) {
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
                for (DataBeanPropertyBuilder property : bean.properties.values()) {
                    String mapped_value = filterProps.keySet().stream()
                            .filter(k -> property.name.equals(k.getSimpleName().toString()))
                            .findAny()
                            .map(k -> filterProps.get(k).getValue().toString())
                            .orElse(null);
                    String key = property.isCollection ?
                            environment.getTypeUtils().erasure(
                                    ((DeclaredType) property.type).getTypeArguments().get(0)
                            ).toString() : (property.isMap ?
                            environment.getTypeUtils().erasure(
                                    ((DeclaredType) property.type).getTypeArguments().get(1)
                            ).toString() :
                            property.type.toString());
                    DataBeanBuilder mappedBean = builder.beansByInterface.get(key);
                    mapped = mappedBean == null || mapped_value == null ? null :
                            mappedBean.properties.values().stream()
                                    .filter(p -> p.enumName.equals(mapped_value))
                                    .findAny()
                                    .orElse(null);
                    if (mapped != null) break;
                }

                property = mapped != null ? mapped : localProperty;
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
            }
        }
    }

    private boolean filter4ResultMap(DataBeanPropertyBuilder property, FetchOptions options) {
        return options == null ||
                !(options.fetchType == DDataFetchType.NO ||
                        options.ignore.contains(property) ||
                        (options.fetchType == DDataFetchType.COLLECTIONS_ARE_NO && property.isCollectionOrMap())
                );
    }

    private boolean filter4FieldsList(DataBeanPropertyBuilder property, FetchOptions options) {
        return options == null ? !property.isCollectionOrMap() :
                !(options.fetchType == DDataFetchType.NO ||
                        options.ignore.contains(property) ||
                        ((
                                options.fetchType == DDataFetchType.COLLECTIONS_ARE_LAZY ||
                                        options.fetchType == DDataFetchType.COLLECTIONS_ARE_NO
                        ) && property.isCollectionOrMap()) ||
                        (options.fetchType == DDataFetchType.LAZY &&
                                builder.beansByInterface.containsKey(property.mappedType.toString())
                        )
                );
    }


    private class FetchOptions {
        final DDataFetchType fetchType;
        final String sqlFrom;
        final String resultMap;
        final String alias;
        final List<DataBeanPropertyBuilder> ignore;

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
        }

        boolean filterBasic(DataBeanPropertyBuilder property) {
            return !this.ignore.contains(property);
        }
    }

    private class MappedTable {
        final int tableIndex;
        final DataBeanPropertyBuilder property;
        final DataBeanBuilder mappedBean;
        final boolean useInFieldsList;

        MappedTable(int i, DataBeanPropertyBuilder p, DataBeanBuilder b, FetchOptions fetchOptions) {
            this.tableIndex = i;
            this.property = p;
            this.mappedBean = b;
            this.useInFieldsList = filter4FieldsList(p, fetchOptions);
        }

        boolean useInFieldsList() {
            return useInFieldsList;
        }
    }
}
