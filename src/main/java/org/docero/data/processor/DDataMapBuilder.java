package org.docero.data.processor;

import org.docero.data.DDataFetchType;
import org.docero.data.DDataFilterOption;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
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
    private final TypeMirror collectionType;
    private final TypeMirror temporalType;
    private final TypeMirror oldDateType;
    private final HashMap<String, Mapping> mappings = new HashMap<>();


    DDataMapBuilder(DDataBuilder builder, ProcessingEnvironment environment) {
        this.builder = builder;
        this.environment = environment;
        collectionType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement("java.util.Collection").asType()
        );
        temporalType =
                environment.getElementUtils().getTypeElement("java.time.temporal.Temporal").asType();
        oldDateType =
                environment.getElementUtils().getTypeElement("java.util.Date").asType();
    }

    void build() throws Exception {
        if (builder.beansByInterface.isEmpty()) return;

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
                createSimpleGetter(mapperRoot, repository);
            } else {
                repositoryNamespace = repository.repositoryInterface.toString();

                if (!repository.hasGet()) {
                    createSimpleGetter(mapperRoot, repository);
                }

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
            if (!mappedBy.collection) {
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
                String mappingKey = beanElement.asType().toString() + "." + method.getSimpleName();
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
                        DataBeanPropertyBuilder property = methodProperty(bean, method);
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

    private DataBeanPropertyBuilder methodProperty(DataBeanBuilder bean, ExecutableElement method) {
        String s = method.getSimpleName().toString();
        String propName = s.startsWith("get") || s.startsWith("set") ?
                Character.toLowerCase(s.charAt(3)) + s.substring(4) :
                Character.toLowerCase(s.charAt(2)) + s.substring(3);
        return bean.properties.values().stream().filter(p -> p.name.equals(propName)).findAny().orElse(null);
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

        System.out.println("build " + methodElement + " in " + repository.repositoryInterface +
                " (" + (methodOpt
                .map(dDataMethodBuilder -> dDataMethodBuilder.methodName + "_" + dDataMethodBuilder.methodIndex)
                .orElse("-unknown-")) +
                ")");

        if (methodOpt.isPresent()) {
            Document doc = mapperRoot.getOwnerDocument();
            DDataMethodBuilder method = methodOpt.get();

            Optional<? extends AnnotationMirror> fetchOpt = methodElement.getAnnotationMirrors().stream()
                    .filter(a -> a.toString().indexOf("_DDataFetch_") > 0)
                    .findAny();
            FetchOptions fetchOptions = null;
            Map<? extends ExecutableElement, ? extends AnnotationValue> fetchProps = null;
            if (fetchOpt.isPresent())
                fetchOptions = new FetchOptions(repository, fetchOpt.get());

            ArrayList<FilterOption> filters = new ArrayList<>();
            for (VariableElement variableElement : methodElement.getParameters()) {
                FilterOption filter = new FilterOption(repository, methodElement, variableElement);
                filters.add(filter);
                /*System.out.println(filter.option + " " + (filter.property == null ?
                        "-none-" :
                        filter.property.dataBean.table + "." + filter.property.columnName));*/
            }

            if (method.returnType == null) {
                org.w3c.dom.Element select = (org.w3c.dom.Element)
                        mapperRoot.appendChild(doc.createElement("insert|update|delete"));
                select.setAttribute("id", method.methodName + "_" + method.methodIndex);
                select.setAttribute("parameterType", "HashMap");
                //TODO update, insert, delete sql
            } else createDeclaredSelect(mapperRoot, method, fetchOptions, filters);
        } else throw new Exception("not found info about method '" + methodElement.getSimpleName() +
                " of " + repository.repositoryInterface);
    }

    private void createDeclaredSelect(
            org.w3c.dom.Element mapperRoot,
            DDataMethodBuilder method,
            FetchOptions fetchOptions, List<FilterOption> filters
    ) {
        DataBeanBuilder bean = builder.beansByInterface.get(method.repositoryBuilder.forInterfaceName());
        AtomicInteger index = new AtomicInteger();
        List<MappedTable> mappedBeans = bean.properties.values().stream()
                .filter(p -> fetchOptions == null || !fetchOptions.ignore.contains(p))
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.typeErasure);
                    return b == null ? null : new MappedTable(index.incrementAndGet(), p, b);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        buildResultMap(mapperRoot, method.repositoryBuilder,
                method.methodName + "_" + method.methodIndex,
                fetchOptions, mappedBeans);

        boolean isList = environment.getTypeUtils().isSubtype(
                environment.getTypeUtils().erasure(method.returnType),
                collectionType);

        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element select = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("select"));
        select.setAttribute("id", method.methodName + "_" + method.methodIndex);
        select.setAttribute("parameterType", "HashMap");
        select.setAttribute("resultMap", method.methodName + "_" + method.methodIndex + "_ResultMap");

        select.appendChild(doc.createTextNode(buildSelectSql(bean, fetchOptions, mappedBeans)));
    }

    private String buildSelectSql(DataBeanBuilder bean, FetchOptions fetchOptions, List<MappedTable> mappedBeans) {
        StringBuilder sql = new StringBuilder();
        sql.append("\nSELECT\n");
        sql.append(bean.properties.values().stream()
                .filter(p -> fetchOptions == null || !fetchOptions.ignore.contains(p))
                .filter(p -> !p.collection).map(p -> "  t0." + p.columnName + " AS _" + p.columnName)
                .collect(Collectors.joining(",\n")));
        mappedBeans.forEach(b ->
                addManagedBeanToSelect(sql, b, fetchOptions));
        sql.append("\nFROM ").append(bean.table).append(" AS t0\n");

        /*sql.append("WHERE\n");
        sql.append(bean.properties.values().stream()
                .filter(p -> p.isId).map(p -> " t0." + p.columnName + " = #{" + p.name +
                        ", javaType=" + (p.type.getKind().isPrimitive() ?
                        environment.getTypeUtils().boxedClass((PrimitiveType) p.type) :
                        p.type) + jdbcTypeFor(p.type) +
                        "}")
                .collect(Collectors.joining(" AND ")));
        sql.append("\n");*/
        return sql.toString();
    }

    private void addManagedBeanToSelect(StringBuilder sql, MappedTable mappedTable, FetchOptions fetchOptions) {
        sql.append(",\n");
        sql.append(mappedTable.mappedBean.properties.values().stream()
                .filter(p -> fetchOptions == null || !fetchOptions.ignore.contains(p))
                .filter(p -> !p.collection).map(p -> "  t" + mappedTable.tableIndex + "." + p.columnName +
                        " AS " + mappedTable.property.name + "_" + p.columnName)
                .collect(Collectors.joining(",\n")));
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
                .filter(p -> !(p.isId || p.collection))
                .filter(p -> fetchOptions == null || !fetchOptions.ignore.contains(p))
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            map.appendChild(doc.createElement("result"));
                    boolean isBean = builder.beansByInterface.containsKey(p.type.toString());
                    id.setAttribute("property", p.name + (isBean ? "_foreignKey" : ""));
                    id.setAttribute("column", "_" + p.columnName);
                });

        mappedBeans.forEach(b ->
                addManagedBeanToResultMap(map, b, fetchOptions));

        if (fetchOptions == null || fetchOptions.fetchType == DDataFetchType.COLLECTIONS_ARE_LAZY) {

        } else if (fetchOptions.fetchType == DDataFetchType.NO) {

        }
    }

    private void addManagedBeanToResultMap(org.w3c.dom.Element map, MappedTable mappedTable, FetchOptions fetchOptions) {
        Document doc = map.getOwnerDocument();
        org.w3c.dom.Element managed = (org.w3c.dom.Element) map.appendChild(doc.createElement(
                mappedTable.property.collection ? "collection" : "association"));
        managed.setAttribute("property", mappedTable.property.name);
        managed.setAttribute("javaType", mappedTable.property.type.toString());

        mappedTable.mappedBean.properties.values().stream()
                .filter(p -> p.isId)
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            managed.appendChild(doc.createElement("id"));
                    id.setAttribute("property", p.name);
                    id.setAttribute("column", mappedTable.property.name + "_" + p.columnName);
                });

        mappedTable.mappedBean.properties.values().stream()
                .filter(p -> !(p.isId || p.collection))
                .filter(p -> fetchOptions == null || !fetchOptions.ignore.contains(p))
                .forEach(p -> {
                    org.w3c.dom.Element id = (org.w3c.dom.Element)
                            managed.appendChild(doc.createElement("result"));
                    boolean isBean = builder.beansByInterface.containsKey(p.type.toString());
                    id.setAttribute("property", p.name + (isBean ? "_foreignKey" : ""));
                    id.setAttribute("column", mappedTable.property.name + "_" + p.columnName);
                });
    }

    private void createSimpleGetter(org.w3c.dom.Element mapperRoot, DataRepositoryBuilder repository) {
        DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName());
        AtomicInteger index = new AtomicInteger();
        List<MappedTable> mappedBeans = bean.properties.values().stream()
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.typeErasure);
                    return b == null ? null : new MappedTable(index.incrementAndGet(), p, b);
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

        select.appendChild(doc.createTextNode(buildSelectSql(bean, null, mappedBeans)));
    }

    private String jdbcTypeFor(TypeMirror type) {
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

    private static AnnotationMirror findAnnotation(Element element, String suffix) {
        for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {
            if (annotationMirror.getAnnotationType().toString().indexOf(suffix) > 0)
                return annotationMirror;
        }
        return null;
    }

    private class FilterOption {
        final DDataFilterOption option;
        final DataBeanPropertyBuilder property;

        FilterOption(DataRepositoryBuilder repository, ExecutableElement methodElement, VariableElement variableElement) {
            DataBeanBuilder bean = builder.beansByInterface.get(repository.forInterfaceName.toString());
            Optional<? extends AnnotationMirror> filterOpt = variableElement.getAnnotationMirrors().stream()
                    .filter(a -> a.toString().indexOf("_Filter_") > 0)
                    .findAny();

            if (filterOpt.isPresent()) {
                AnnotationMirror filterMirror = filterOpt.get();
                Map<? extends ExecutableElement, ? extends AnnotationValue> filterProps =
                        environment.getElementUtils().getElementValuesWithDefaults(filterMirror);
                //System.out.println("filter: " + filterProps);

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
                    String key = property.collection ?
                            environment.getTypeUtils().erasure(
                                    ((DeclaredType) property.type).getTypeArguments().get(0)
                            ).toString() :
                            property.type.toString();
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
    }

    private class MappedTable {
        final int tableIndex;
        final DataBeanPropertyBuilder property;
        final DataBeanBuilder mappedBean;

        MappedTable(int i, DataBeanPropertyBuilder p, DataBeanBuilder b) {
            this.tableIndex = i;
            this.property = p;
            this.mappedBean = b;
        }
    }
}
