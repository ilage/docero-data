package org.docero.data.processor;

import org.docero.data.DDataFetchType;
import org.docero.data.DDataFilterOption;
import org.docero.data.DictionaryType;
import org.docero.data.utils.DDataException;
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
import java.io.IOException;
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
            TypeElement keyElement = builder.environment.getElementUtils().getTypeElement(bean.keyType);
            if (keyElement == null) throw new RuntimeException("No element found for " + bean.keyType);
            if (repositoryElement == null) {
                // методов с фильтрами нет, можно строить репозиторий, для мультиклассовых
                repository.build(environment, builder.spring);

                buildDDLasComment(bean, mapperRoot);

                repositoryNamespace = repository.forInterfaceName();
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.GET, keyElement),
                        defaultFetchOptions, repository);
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.INSERT, keyElement),
                        defaultFetchOptions, repository);
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.UPDATE, keyElement),
                        defaultFetchOptions, repository);
                createDefinedMethod(mapperRoot,
                        new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.DELETE, keyElement),
                        defaultFetchOptions, repository);
                if (bean.isDictionary())
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.SELECT, keyElement),
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

                buildDDLasComment(bean, mapperRoot);

                for (Element methodElement : repositoryElement.getEnclosedElements())
                    if (methodElement.getKind() == ElementKind.METHOD && !methodElement.getModifiers().contains(Modifier.STATIC) ?
                            !methodElement.getModifiers().contains(Modifier.DEFAULT) :
                            methodElement.getModifiers().contains(Modifier.ABSTRACT))
                        createDefinedMethod(mapperRoot, (ExecutableElement) methodElement, repository);

                if (repository.defaultGetMethod == null)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.GET, keyElement),
                            defaultFetchOptions, repository);
                if (!repository.hasInsert)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.INSERT, keyElement),
                            defaultFetchOptions, repository);
                if (!repository.hasUpdate)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.UPDATE, keyElement),
                            defaultFetchOptions, repository);
                if (repository.defaultDeleteMethod == null)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.DELETE, keyElement),
                            defaultFetchOptions, repository);
                if (bean.isDictionary() && repository.defaultListMethod == null)
                    createDefinedMethod(mapperRoot,
                            new DDataMethodBuilder(repository, bean, DDataMethodBuilder.MType.SELECT, keyElement),
                            defaultFetchOptions, repository);
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

        /*
            class DData - static methods for accessing library classes
        */
        try (JavaClassWriter cf = new JavaClassWriter(environment, builder.basePackage + ".DDataModule")) {
            cf.println("package " + builder.basePackage + ";");
            cf.println("import org.apache.ibatis.session.SqlSessionFactory;");
            cf.println("import org.docero.data.utils.DDataException;");
            if (builder.spring) cf.println("import org.mybatis.spring.support.SqlSessionDaoSupport;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public class DDataModule implements org.docero.data.utils.DDataModule {");

            cf.startBlock("private final static java.util.HashMap<Class<?>,Class<?>> repositories = new java.util.HashMap<Class<?>,Class<?>>() {{");
            for (String repositoryFor : builder.repositoriesByBean.keySet()) {
                DataRepositoryBuilder repository = builder.repositoriesByBean.get(repositoryFor);
                cf.println("put(" + repositoryFor + ".class, " + repository.daoClassName + ".class);");
            }
            for (DataRepositoryBuilder repository : builder.repositories)
                if (!repository.repositoryInterface.toString().startsWith("org.docero.data.DData")) {
                    cf.println("put(" + repository.repositoryInterface + ".class, " + repository.daoClassName + ".class);");
                }
            for (BatchRepositoryBuilder repository : builder.batchRepositories)
                cf.println("put(" + repository.repositoryInterface + ".class, " + repository.implClassName + ".class);");
            cf.endBlock("}};");

            cf.println("");
            cf.startBlock("public java.util.Map<Class,Class> getRepositories() {");
            cf.println("return java.util.Collections.unmodifiableMap(repositories);");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public <T extends java.io.Serializable, C extends java.io.Serializable, R extends org.docero.data.DDataRepository<T, C>>" +
                    " Class<R> getBeanRepositoryInterface(Class<T> beanInterface) {");
            cf.println("return (Class<R>) repositories.get(beanInterface);");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public <T extends java.io.Serializable,C extends java.io.Serializable> " +
                    "org.docero.data.DDataRepository<T,C> getBeanRepository(Class<T> beanInterface, SqlSessionFactory sqlSessionFactory) {");
            cf.println("org.docero.data.DDataRepository<T, C> r = null;");
            cf.println("Class<?> i = repositories.get(beanInterface);");
            cf.startBlock("if(i != null) try {");
            cf.println("r = (org.docero.data.DDataRepository<T, C>) i.newInstance();");
            if (builder.spring)
                cf.println("((SqlSessionDaoSupport) r).setSqlSessionFactory(sqlSessionFactory);");
            cf.endBlock("}");
            cf.startBlock("catch (InstantiationException | IllegalAccessException e) {");
            cf.println("throw new RuntimeException(\"can,t instantiate \" + i.getName(), e);");
            cf.endBlock("}");
            cf.println("return r;");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public <R> R getRepositoryByInterface(Class<R> repositoryInterface, SqlSessionFactory sqlSessionFactory) {");
            cf.println("R r = null;");
            cf.println("Class<?> i = repositories.get(repositoryInterface);");
            cf.startBlock("if(i != null) try {");
            cf.println("r = (R) i.newInstance();");
            if (builder.spring)
                cf.println("((SqlSessionDaoSupport) r).setSqlSessionFactory(sqlSessionFactory);");
            cf.println("return r;");
            cf.endBlock("}");
            cf.startBlock("catch (InstantiationException | IllegalAccessException e) {");
            cf.println("throw new RuntimeException(\"can,t instantiate \" + i.getName(), e);");
            cf.endBlock("}");
            cf.println("return null;");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("private static final java.util.Map<Class<?>,Class<?>> implementations = " +
                    "new java.util.HashMap<Class<?>,Class<?>>() {{");
            for (String interfaceName : builder.beansByInterface.keySet()) {
                cf.println("this.put(" +
                        interfaceName + ".class," +
                        builder.beansByInterface.get(interfaceName).getImplementationName() + ".class);");
            }
            cf.endBlock("}};");
            cf.startBlock("public java.util.Map<Class<?>, Class<?>> getImplementations() {");
            cf.println("return implementations;");
            cf.endBlock("}");

            cf.startBlock("public <T extends java.io.Serializable, C extends java.io.Serializable> T save(");
            cf.println("T t,");
            cf.println("Class beanInterface,");
            cf.println("org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory,");
            cf.println("org.docero.data.utils.UpdateOptions updateOptions,");
            cf.println("org.docero.data.DData dData");
            cf.endBlock(")");
            cf.startBlock("{");
            cf.startBlock("if (dData.getBeanRepository(beanInterface) instanceof AbstractModuleRepository) {");
            cf.println("    AbstractModuleRepository beanRepository = (AbstractModuleRepository) dData.getBeanRepository(beanInterface);");
            cf.println("    C key = (C) beanRepository.save(t, updateOptions, dData, new java.util.HashSet());");
            cf.println("    if (key != null)");
            cf.println("       return (T) beanRepository.get(key);");
            cf.println("    else");
            cf.println("       return t;");
            cf.endBlock("} else return null;");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("private static final String[] cacheNames = new String[] {");
            String cnames = builder.beansByInterface.values().stream()
                    .filter(DataBeanBuilder::isDictionary)
                    .map(b -> "\"" + b.cacheMap + "\"")
                    .collect(Collectors.joining(",\n\t\t"));
            cf.print("\"ddata.dictionaries\"");
            cf.println((cnames.isEmpty() ? "" : ",\n\t") + cnames);
            cf.endBlock("};");
            cf.println("public String[] getCacheNames() {return cacheNames;}");

            if (environment.getElementUtils().getTypeElement("com.fasterxml.jackson.databind.JsonDeserializer") != null) {
                cf.println("");
                cf.startBlock("/**");
                cf.println("simple usage: DData.deserializers.forEach(builder::deserializerByType);");
                cf.println("<p>where builder is org.springframework.http.converter.json.Jackson2ObjectMapperBuilder</p>");
                cf.endBlock("*/");
                cf.startBlock("private static final java.util.Map<Class<?>,com.fasterxml.jackson.databind.JsonDeserializer<?>> deserializers = " +
                        "new java.util.HashMap<Class<?>,com.fasterxml.jackson.databind.JsonDeserializer<?>>() {{");

                HashMap<String, List<DataBeanBuilder>> discriminatedBeans = new HashMap<>();
                for (String interfaceName : builder.beansByInterface.keySet()) {
                    DataBeanBuilder bean = builder.beansByInterface.get(interfaceName);
                    if (bean.abstractBean) {
                        if (bean.discriminatorProperty != null)
                            discriminatedBeans.computeIfAbsent(bean.getTableRef(), (k) -> new ArrayList<>()).add(bean);
                    } else {
                        if (bean.discriminatorProperty != null)
                            discriminatedBeans.computeIfAbsent(bean.getTableRef(), (k) -> new ArrayList<>()).add(bean);

                        cf.startBlock("this.put(" +
                                interfaceName + ".class, new com.fasterxml.jackson.databind.JsonDeserializer<" +
                                interfaceName + ">() {");
                        cf.println("@Override");
                        cf.startBlock("public " + interfaceName + " deserialize(com.fasterxml.jackson.core.JsonParser p, " +
                                "com.fasterxml.jackson.databind.DeserializationContext ctxt) throws " +
                                "java.io.IOException, com.fasterxml.jackson.core.JsonProcessingException {");
                        cf.println("return ctxt.readValue(p, " +
                                bean.getImplementationName() +
                                ".class);");
                        cf.endBlock("}");
                        cf.endBlock("});");
                    }
                }
                for (String table : discriminatedBeans.keySet()) {
                    List<DataBeanBuilder> beans = discriminatedBeans.get(table);
                    DataBeanBuilder prototype = beans.stream().filter(b -> b.abstractBean)
                            .findAny().orElseThrow(() -> new IOException("can't find prototype for " + table));
                    String interfaceName = prototype.interfaceType.toString();
                    cf.startBlock("this.put(" +
                            interfaceName + ".class, new com.fasterxml.jackson.databind.JsonDeserializer<" +
                            interfaceName + ">() {");
                    cf.println("@Override");
                    cf.startBlock("public " + interfaceName + " deserialize(com.fasterxml.jackson.core.JsonParser p, " +
                            "com.fasterxml.jackson.databind.DeserializationContext ctxt) throws " +
                            "java.io.IOException, com.fasterxml.jackson.core.JsonProcessingException {");
                    cf.println("com.fasterxml.jackson.databind.ObjectMapper mapper = (com.fasterxml.jackson.databind.ObjectMapper) p.getCodec();");
                    cf.println("com.fasterxml.jackson.databind.node.ObjectNode root = mapper.readTree(p);");
                    cf.println("if (root.isNull()) return null;");
                    if (prototype.discriminatorProperty == null)
                        throw new DDataException("Bean " + prototype.interfaceType +
                                " has no descriminator property, but other beans extends it as prototype");
                    cf.startBlock("if (root.has(\"" + prototype.discriminatorProperty.name + "\")) {");
                    {
                        cf.println("String val = root.get(\"" + prototype.discriminatorProperty.name + "\").asText();");
                        for (DataBeanBuilder bean : beans)
                            if (!bean.abstractBean) {
                                cf.println("if (val.equals(\"" + bean.discriminatorValue + "\")) " +
                                        "return mapper.readValue(root.toString(), " +
                                        bean.getImplementationName() +
                                        ".class);");
                            }
                        cf.endBlock("}");
                    }
                    cf.println("throw new java.io.IOException(\"can't read object implements '" +
                            interfaceName + "' incorrect value of '" + prototype.discriminatorProperty.name + "'\");");
                    cf.endBlock("}");
                    cf.endBlock("});");
                }
                cf.endBlock("}};");
                cf.startBlock("public java.util.Map<Class<?>, com.fasterxml.jackson.databind.JsonDeserializer<?>> getDeserializers() {");
                cf.println("return deserializers;");
                cf.endBlock("}");
            }

            cf.println("");
            cf.startBlock("public String[] resources() {");
            cf.startBlock("return new String[] {");
            for (DataRepositoryBuilder repository : builder.repositories)
                cf.println("\"classpath:" +
                        repository.mappingClassName.replaceAll("\\.", "/") +
                        ".xml\",");
            for (String umf : userMappingFiles) {
                cf.println("\"classpath:" +
                        umf.replaceAll("\\.", "/") +
                        ".xml\",");
            }
            cf.println("\"classpath:/org/docero/data/simple_maps.xml\"");
            cf.endBlock("};");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public void register(org.docero.data.DData dData, org.docero.data.DDataDictionariesService ds) {");
            for (DataBeanBuilder bean : builder.beansByInterface.values())
                if (bean.dictionary != DictionaryType.NO) {
                    //DataRepositoryBuilder r = builder.repositoriesByBean.get(bean.interfaceType.toString());
                    cf.println("ds.registerAsDictionary(dData.getBeanRepository(" +
                            bean.interfaceType + ".class), " +
                            bean.interfaceType + ".class, " +
                            bean.getImplementationName() + ".class);");
                }
            cf.endBlock("}");

            cf.endBlock("}");
        }

        if (builder.spring) {
            try (JavaClassWriter cf = new JavaClassWriter(environment, builder.basePackage + ".DDataConfiguration")) {
                cf.println("package " + builder.basePackage + ";");
                for (String pkg : builder.packages) cf.println("import " + pkg + ".*;");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                //cf.println("@org.springframework.context.annotation.DependsOn(\"dData\")");
                cf.println("@org.springframework.context.annotation.Configuration");
                cf.startBlock("public class DDataConfiguration {");

                for (DataRepositoryBuilder repository : builder.repositories) {
                    cf.println("@org.springframework.context.annotation.Bean");
                    cf.startBlock("public " + repository.daoClassName + " " + repository.repositoryVariableName +
                            "(org.docero.data.DData dData) {");
                    DeclaredType getType = environment.getTypeUtils().getDeclaredType(
                            environment.getElementUtils().getTypeElement("org.docero.data.DDataRepository"),
                            repository.forInterfaceName, repository.idClass);

                    cf.println(getType + " r = dData.getBeanRepository(" + repository.forInterfaceName + ".class);");
                    cf.println("return (" + repository.daoClassName + ") r;");
                    cf.endBlock("}");
                }

                for (BatchRepositoryBuilder repository : builder.batchRepositories) {
                    String daoInterfaceName = repository.repositoryInterface.toString();
                    int offset = daoInterfaceName.lastIndexOf('.') + 1;
                    String methodName = Character.toLowerCase(daoInterfaceName.charAt(offset)) +
                            daoInterfaceName.substring(offset + 1);
                    cf.println("@org.springframework.context.annotation.Bean");
                    cf.startBlock("public " + daoInterfaceName + " " + methodName +
                            "(org.docero.data.DData dData) throws org.docero.data.utils.DDataException {");
                    cf.println("return dData.getRepository(" + daoInterfaceName + ".class);");
                    cf.endBlock("}");
                }

                cf.endBlock("}");
            }
        }

        Set<String> setOfJaxbEnabledPackages = builder.beansByInterface.values().stream()
                .map(b -> b.interfaceType.toString())
                .map(bi -> bi.substring(0, bi.lastIndexOf('.')))
                .collect(Collectors.toSet());
        for (String packageName : setOfJaxbEnabledPackages)
            buildObjectFactoryForPackage(packageName);

        return true;
    }

    private void buildObjectFactoryForPackage(String packageName) throws IOException {
        List<DataBeanBuilder> pkgBeans = builder.beansByInterface.values().stream()
                .filter(b -> b.interfaceType.toString().startsWith(packageName))
                .collect(Collectors.toList());

        try (JavaClassWriter cf = new JavaClassWriter(environment, packageName + ".ObjectFactory")) {
            cf.println("package " + packageName + ";");
            cf.println("");
            cf.println("import javax.xml.bind.JAXBElement;");
            cf.println("import javax.xml.bind.annotation.XmlElementDecl;");
            cf.println("import javax.xml.bind.annotation.XmlRegistry;");
            cf.println("import javax.xml.namespace.QName;");
            cf.println("");

            cf.println("@XmlRegistry");
            cf.startBlock("public class ObjectFactory {");
            for (DataBeanBuilder bean : pkgBeans) {
                String shortName = bean.interfaceType.toString();
                shortName = shortName.substring(shortName.lastIndexOf('.') + 1);
                String namespace = bean.xmlNamespace == null ? "" : bean.xmlNamespace;

                cf.println("");
                cf.println("private final static QName _" + shortName +
                        "_QNAME = new QName(\"" + namespace + "\", \"" + shortName + "\");");
                cf.println("");
                cf.startBlock("public " + bean.getImplementationName() + " create" + shortName + "() {");
                cf.println("return new " + bean.getImplementationName() + "();");
                cf.endBlock("}");
                cf.println("");
                cf.println("@XmlElementDecl(namespace = \"\", name = \"" + shortName + "\")");
                cf.startBlock("public JAXBElement<" + bean.getImplementationName() + "> create" + shortName +
                        " (" + bean.getImplementationName() + " value) {");
                cf.println("return new JAXBElement<>(_" + shortName + "_QNAME, " +
                        bean.getImplementationName() + ".class, null, value);");
                cf.endBlock("}");
            }

            cf.endBlock("}");
        }
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

        if (method.selectId == null && method.insertId == null && method.updateId == null && method.deleteId == null) {
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
        if (method.insertId != null)
            userMappingFiles.add(method.insertId.substring(0, method.insertId.lastIndexOf('.')));
        if (method.updateId != null)
            userMappingFiles.add(method.updateId.substring(0, method.updateId.lastIndexOf('.')));
        if (method.deleteId != null)
            userMappingFiles.add(method.deleteId.substring(0, method.deleteId.lastIndexOf('.')));

        List<FilterOption> filters = method.getFilters();
        VariableElement order = method.getOrder();

        Document doc = mapperRoot.getOwnerDocument();
        DataBeanBuilder bean = builder.beansByInterface.get(method.repositoryBuilder.forInterfaceName());

        AtomicInteger index = new AtomicInteger();
        CopyOnWriteArrayList<MappedTable> mappedTables = new CopyOnWriteArrayList<>(bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .map(p -> {
                    DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                    if (b == null) return null;
                    else {
                        Mapping mapping = builder.mappings.get(p.dataBean.interfaceType + "." + p.name);
                        return new MappedTable(0, index.incrementAndGet(), p, b,
                                mapping != null && mapping.alwaysLazy ? fetchOptions.withLazy() : fetchOptions,
                                filters);
                    }
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
                if (!method.returnSimpleType)
                    buildResultMap(mapperRoot, repository, methodName, fetchOptions, mappedTables, filters);

                if (method.counterParameter != null) {
                    org.w3c.dom.Element selectCount = (org.w3c.dom.Element)
                            mapperRoot.appendChild(doc.createElement("select"));

                    selectCount.setAttribute("resultType", "java.lang.Integer");
                    selectCount.setAttribute("parameterType", "HashMap");
                    selectCount.setAttribute("id", methodName + "_count");
                    ///////////////////////////////////////////////////////////////////////////////
                    if (bean.versionalType != null) addSqlVersionValue(bean, filters, selectCount);
                    //Запускаем чтобы сформировать все необходимое для count(*)
                    buildSqlForCount(repository, method, bean, fetchOptions, mappedTables, selectCount, filters);
                }
                /////////////////////////////////////////////////////////////////////////////////////////////////
                org.w3c.dom.Element select = (org.w3c.dom.Element)
                        mapperRoot.appendChild(doc.createElement("select"));
                select.setAttribute("id", methodName);
                select.setAttribute("parameterType", "HashMap");
                if (method.returnType != null && method.returnSimpleType)
                    select.setAttribute("resultType", method.returnType.toString());
                else
                    select.setAttribute("resultMap", methodName + "_ResultMap");

                if (bean.versionalType != null) addSqlVersionValue(bean, filters, select);

                // Запустится ПОСЛЕ формирования COUNT(*) за один проход
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

    private void addSqlVersionValue(DataBeanBuilder bean, List<FilterOption> filters, org.w3c.dom.Element selectElement) {
        StringBuilder sql = new StringBuilder();
        bean.properties.values().stream().filter(p -> p.isVersionFrom).findAny().ifPresent(p -> {
            String codedParameter = filters.stream()
                    .filter(f -> "VERSION_".equals(f.enumName))
                    .findAny().map(this::buildSqlParameter)
                    .orElse(buildSqlParameter(bean, p));

            sql.append("\nWITH tt AS (SELECT ");
            if (environment.getTypeUtils().directSupertypes(p.type).stream()
                    .anyMatch(c -> c.toString().equals(builder.temporalType.toString())) ||
                    environment.getTypeUtils().isSubtype(p.type, builder.oldDateType))
                sql.append("CAST(").append(codedParameter).append(" AS TIMESTAMP)");
            else
                sql.append(codedParameter);
            sql.append(" AS t)");
        });
        selectElement.appendChild(selectElement.getOwnerDocument().createTextNode(sql.toString()));
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
                    .filter(p -> mappedTables.stream().noneMatch(m ->
                            m.property.columnName.equals(p.columnName) &&
                                    m.property.mappedType.toString().equals(p.mappedType.toString())))
                    .map(p -> {
                        DataBeanBuilder b = builder.beansByInterface.get(p.mappedType.toString());
                        if (b == null) return null;
                        else {
                            Mapping mapping = builder.mappings.get(p.dataBean.interfaceType + "." + p.name);
                            return new MappedTable(0, index.incrementAndGet(), p, b,
                                    mapping != null && mapping.alwaysLazy ? fetchOptions.withLazy() : fetchOptions,
                                    null);
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            mappedTables.addAll(beanTables);
        }
    }

    private void buildDDLasComment(
            DataBeanBuilder bean,
            org.w3c.dom.Element domElement
    ) {
        List<DataBeanPropertyBuilder> orderedIds = bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::isId)
                .sorted((p1, p2) -> {
                    if (p1.type.getKind().isPrimitive())
                        return p2.type.getKind().isPrimitive() ? p1.name.compareTo(p2.name) : -1;
                    else {
                        String t1 = p1.type.toString();
                        String t2 = p2.type.toString();
                        if (t1.startsWith("java.lang."))
                            return t2.startsWith("java.lang.") ? p1.name.compareTo(p2.name) : -1;
                        if (t1.startsWith("java.util."))
                            return t2.startsWith("java.util.") ? p1.name.compareTo(p2.name) : -1;
                        return p1.name.compareTo(p2.name);
                    }
                })
                .collect(Collectors.toList());

        Document doc = domElement.getOwnerDocument();
        StringBuilder comment = new StringBuilder(" Example of DDL statement:\nCREATE TABLE ");
        comment.append(bean.getTableRef()).append(" (\n");

        comment.append(orderedIds.stream()
                .map(p -> "\t" + p.getColumnRef() + " " + p.jdbcType + " NOT NULL,\n")
                .collect(Collectors.joining()));

        comment.append(bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notId)
                .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                .filter(this::notManagedBean)
                .map(p -> "\t" + p.getColumnRef() + " " + (
                                "VARCHAR".equals(p.jdbcType) ?
                                        (p.length != 0 ? "VARCHAR(" + p.length + ")" : "TEXT") :
                                        p.jdbcType
                        ) + (
                                !p.nullable || p.type.getKind().isPrimitive() ? " NOT NULL" : ""
                        ) + ",\n"
                ).collect(Collectors.joining()));

        comment.append("\tCONSTRAINT ").append(bean.name).append("_pkey PRIMARY KEY (");
        comment.append(orderedIds.stream()
                .map(DataBeanPropertyBuilder::getColumnRef)
                .collect(Collectors.joining(",")));
        comment.append(")\n");

        comment.append(") TABLESPACE pg_default;\n");
        comment.append("ALTER TABLE ").append(bean.getTableRef()).append(" OWNER to postgres;\n\n");

        bean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notCollectionOrMap)
                .filter(this::isManagedBean)
                .forEach(p -> {
                    Mapping mapping = builder.mappings.get(bean.interfaceType + "." + p.name);
                    if (mapping != null) comment.append("ALTER TABLE ")
                            .append(bean.getTableRef())
                            .append(" ADD CONSTRAINT ").append(bean.name)
                            .append("_").append(p.columnName)
                            .append("_fkey FOREIGN KEY (")
                            .append(mapping.properties.stream()
                                    .map(DataBeanPropertyBuilder::getColumnRef)
                                    .collect(Collectors.joining(",")))
                            .append(")\n\t  REFERENCES ").append(mapping.mappedProperties.get(0).dataBean.getTableRef())
                            .append(" (")
                            .append(mapping.mappedProperties.stream()
                                    .map(DataBeanPropertyBuilder::getColumnRef)
                                    .collect(Collectors.joining(","))
                            ).append(");\n");
                });

        domElement.appendChild(doc.createComment(comment.toString()));
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
        switch (method.methodType) {
            case SELECT:
            case GET:
                sql.append("\nSELECT\n");
                if (bean.versionalType != null) sql.append("  tt.t AS dDataBeanActualAt_,\n");

                HashMap<String, DataBeanPropertyBuilder> allProperties = new HashMap<>();
                allProperties.putAll(bean.properties);
                if (repository.discriminator != null)
                    buildPropertiesOfDiscriminatedBean(repository, allProperties);

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
                            .filter(MappedTable::useInFieldsListOrFilters)
                            .forEach(t -> addManagedBeanToFrom(sql, t, fetchOptions));
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
            case GET:
                sql.append("\nFROM ").append(bean.getTableRef()).append(" AS t0\n");
                if (bean.versionalType != null) sql.append("CROSS JOIN tt\n");
            case UPDATE:
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
                        addFiltersToSql(domElement, sql, bean, method, mappedTables, filters, true);
                } else {
                    if (method.methodType == DDataMethodBuilder.MType.GET)
                        addJoins(mappedTables, sql, null);

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
                sql.append("\n FROM (SELECT * FROM ").append(bean.getTableRef()).append(" AS t0\n");
                if (bean.versionalType != null) sql.append("CROSS JOIN tt\n");

                domElement.appendChild(doc.createTextNode(sql.toString()));

                StringBuilder psql = new StringBuilder();
                FilterOption versionParameter = addFiltersToSql(domElement, psql, bean, method, mappedTables, filters, false);
                domElement.appendChild(doc.createTextNode(psql.toString()));

                StringBuilder ssql = new StringBuilder();
                if (order != null)
                    addOrder(order, domElement);
                else if (!fetchOptions.order.isEmpty()) ssql
                        .append("\tORDER BY ")
                        .append(fetchOptions.order.keySet().stream()
                                .map(o -> o.getColumnRef() + " " + fetchOptions.order.get(o))
                                .collect(Collectors.joining(", ")));

                DDataMethodBuilder.DDataMethodParameter rbParam = method.parameters.stream()
                        .filter(p -> environment.getTypeUtils().isSameType(builder.rowBoundsType, p.type))
                        .findAny().orElse(null);//
                if (rbParam != null) {
                    domElement.appendChild(doc.createTextNode(ssql.toString()));
                    ssql = new StringBuilder();
                    addRowBounds(rbParam, domElement);
                }

                ssql.append("\n) AS t0\n");

                if (bean.versionalType != null) ssql.append("CROSS JOIN tt\n");
                addJoins(mappedTables, ssql, versionParameter);

                domElement.appendChild(doc.createTextNode(ssql.toString()));
                ssql = new StringBuilder();

                if (order != null) {
                    addOrder(order, domElement);
                } else if (!fetchOptions.order.isEmpty()) ssql
                        .append("\tORDER BY ")
                        .append(fetchOptions.order.keySet().stream()
                                .map(o -> "t0." + o.getColumnRef() + " " + fetchOptions.order.get(o))
                                .collect(Collectors.joining(", ")));
                ssql.append('\n');
                domElement.appendChild(doc.createTextNode(ssql.toString()));
        }
    }

    private void buildPropertiesOfDiscriminatedBean(
            DataRepositoryBuilder repository,
            HashMap<String, DataBeanPropertyBuilder> allProperties
    ) {
        assert repository.discriminator != null;
        for (DataRepositoryDiscriminator.Item item : repository.discriminator.beans) {
            DataBeanBuilder subBean = builder.beansByInterface.get(item.beanInterface);
            subBean.properties.forEach((name, builder) -> {
                if (!allProperties.containsKey(name))
                    allProperties.put(name, builder);
            });
        }
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Используется для формирования метода list_N_count.
     * Параметры для list_N_count копируются из сооотвествующего метода list_N.
     * Данный метод запускется не всегда, а при условии, что надо формировать
     * count(...) для соответствующего метода list(...), т.е. данный list(...) имеет параметр типа DDataAddCountType).
     *
     * @param repository
     * @param method
     * @param bean
     * @param fetchOptions
     * @param mappedTables
     * @param domElement
     * @param filters
     */
    private void buildSqlForCount(
            DataRepositoryBuilder repository,
            DDataMethodBuilder method,
            DataBeanBuilder bean,
            FetchOptions fetchOptions,
            List<MappedTable> mappedTables,
            org.w3c.dom.Element domElement,
            List<FilterOption> filters


    ) {
        Document doc = domElement.getOwnerDocument();

        StringBuilder sql = new StringBuilder();

        sql.append("\n");
        if (bean.versionalType != null) sql.append("  tt.t AS dDataBeanActualAt_,\n");

        HashMap<String, DataBeanPropertyBuilder> allProperties = new HashMap<>();
        allProperties.putAll(bean.properties);
        if (repository.discriminator != null)
            buildPropertiesOfDiscriminatedBean(repository, allProperties);

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
                    .filter(MappedTable::useInFieldsListOrFilters)
                    .forEach(t -> addManagedBeanToFrom(sql, t, fetchOptions));

        StringBuilder sql1 = new StringBuilder();
        sql1.append("\nSELECT COUNT(*) ");
        sql1.append("\n FROM ").append(bean.getTableRef()).append(" AS t0\n");
        if (bean.versionalType != null) sql1.append("CROSS JOIN tt\n");

        domElement.appendChild(doc.createTextNode(sql1.toString()));

        StringBuilder psql = new StringBuilder();
        FilterOption versionParameter = addFiltersToSql(domElement, psql, bean, method, mappedTables, filters, false);
        domElement.appendChild(doc.createTextNode(psql.toString()));

        StringBuilder ssql = new StringBuilder();

        if (bean.versionalType != null) ssql.append("CROSS JOIN tt\n");
        addJoins(mappedTables.stream().filter(MappedTable::useInFilters).collect(Collectors.toList()), ssql, versionParameter);

        domElement.appendChild(doc.createTextNode(ssql.toString()));
        ssql = new StringBuilder();

                /*if (order != null) {
                    addOrder(order, domElement);
                } else if (!fetchOptions.order.isEmpty()) ssql
                        .append("\tORDER BY ")
                        .append(fetchOptions.order.keySet().stream()
                                .map(o -> "t0." + o.getColumnRef() + " " + fetchOptions.order.get(o))
                                .collect(Collectors.joining(", ")));*/
        ssql.append('\n');
        domElement.appendChild(doc.createTextNode(ssql.toString()));
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
                    sk.setAttribute("keyProperty", prop.name);
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
        return fetchOptions.exclusively.isEmpty() ?
                fetchOptions.ignore.stream().noneMatch(f -> f.name.equals(p.name)) :
                fetchOptions.exclusively.stream().anyMatch(f -> f.name.equals(p.name));
    }

    private boolean notManagedBean(DataBeanPropertyBuilder propertyBuilder) {
        //return !this.builder.beansByInterface.containsKey(propertyBuilder.type.toString());
        return this.builder.isSimpleMappedType(propertyBuilder.mappedType);
    }

    private boolean isManagedBean(DataBeanPropertyBuilder propertyBuilder) {
        //return !this.builder.beansByInterface.containsKey(propertyBuilder.type.toString());
        return !this.builder.isSimpleMappedType(propertyBuilder.mappedType);
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

    private FilterOption addFiltersToSql(
            org.w3c.dom.Element domElement,
            StringBuilder sql,
            DataBeanBuilder bean,
            DDataMethodBuilder method,
            List<MappedTable> mappedTables,
            List<FilterOption> filters,
            boolean addJoins
    ) {
        Document doc = domElement.getOwnerDocument();

        List<org.w3c.dom.Element> where = new ArrayList<>();

        //HashSet<MappedTable> joins = new HashSet<>();
        //joins.addAll(mappedTables.stream().filter(MappedTable::useInFieldsList).collect(Collectors.toList()));

        HashMap<String, IfExists> whereExists = new HashMap<>();
        org.w3c.dom.Element e;
        FilterOption versionParameter = null;
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

                String filterParameter = filter.property.getColumnWriter(
                        jdbcTypeParameterFor(filter.parameter, filter.variableType));

                boolean isCaseIndependentLike = false;
                if ("VERSION_".equals(filter.enumName)) {
                    DataBeanPropertyBuilder versionToProperty = filter.property.dataBean.properties.values().stream()
                            .filter(p -> p.isVersionTo).findAny().orElse(null);
                    e = doc.createElement("if");
                    e.setAttribute("test", filter.parameter + " != null");
                    e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                            filter.property.getColumnRef() + " <= " + filterParameter +
                            (versionToProperty != null ?
                                    (" AND (t" + tIdx + "." +
                                            versionToProperty.getColumnRef() + " > " + filterParameter +
                                            " OR t" + tIdx + "." +
                                            versionToProperty.getColumnRef() + " IS NULL)\n") :
                                    ("")
                            )
                    ));
                    versionParameter = filter;
                } else switch (filter.option) {
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
                        e_in.appendChild(doc.createTextNode(jdbcTypeParameterFor("item", itemType)));
                        break;

                    case ILIKE_HAS:
                        isCaseIndependentLike = true;
                    case LIKE_HAS:
                        if ("java.lang.String".equals(filter.variableType.toString())) {
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
                                    (isCaseIndependentLike ? " ILIKE " : " LIKE ") +
                                    jdbcTypeParameterFor("item", filter.variableType)));
                        } else {
                            e = makeSimilarTo(filter, doc, tIdx, isCaseIndependentLike);
                        }
                        break;
                    case ILIKE_HAS_ALL:
                        isCaseIndependentLike = true;
                    case LIKE_HAS_ALL:
                        if ("java.lang.String".equals(filter.variableType.toString())) {
                            e = doc.createElement("if");
                            e.setAttribute("test", filter.parameter + " != null");
                            org.w3c.dom.Element e_like = (org.w3c.dom.Element)
                                    e.appendChild(doc.createElement("foreach"));
                            e_like.setAttribute("item", "item");
                            e_like.setAttribute("index", "index");
                            e_like.setAttribute("collection", filter.parameter);
                            e_like.setAttribute("open", "AND ");
                            e_like.setAttribute("close", "");
                            e_like.setAttribute("separator", " AND ");
                            e_like.appendChild(doc.createTextNode("t" +
                                    tIdx + "." + filter.property.getColumnRef() +
                                    (isCaseIndependentLike ? " ILIKE " : " LIKE ") +
                                    jdbcTypeParameterFor("item", filter.variableType)));
                        } else {
                            e = makeSimilarTo(filter, doc, tIdx, isCaseIndependentLike);
                        }

                        break;
                    case ILIKE:
                    case ILIKE_STARTS:
                    case ILIKE_ENDS:
                        isCaseIndependentLike = true;
                    case LIKE:
                    case LIKE_STARTS:
                    case LIKE_ENDS:
                        if ("java.lang.String".equals(filter.variableType.toString())) {
                            e = doc.createElement("if");
                            e.setAttribute("test", filter.parameter + " != null");
                            e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                                    filter.property.getColumnRef() +
                                    (isCaseIndependentLike ? " ILIKE " : " LIKE ") +
                                    filterParameter + "\n"));
                        } else {
                            e = makeSimilarTo(filter, doc, tIdx, isCaseIndependentLike);
                        }
                        break;
                    default:
                        e = null;
                }

                if (e != null)
                    if (currentJoin != null && (!addJoins || !currentJoin.useInFieldsList || method.methodType == DDataMethodBuilder.MType.DELETE)) {
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
        if (addJoins && method.methodType != DDataMethodBuilder.MType.DELETE)
            addJoins(mappedTables, sql, versionParameter);

        //if (method.methodType == DDataMethodBuilder.MType.SELECT || method.methodType == DDataMethodBuilder.MType.GET) {
        domElement.appendChild(doc.createTextNode(sql.toString()));
        if (where.size() > 0) {
            org.w3c.dom.Element whereElt = doc.createElement("trim");
            whereElt.setAttribute("prefix", "WHERE");
            whereElt.setAttribute("prefixOverrides", "AND ");
            domElement.appendChild(whereElt);
            where.forEach(whereElt::appendChild);
        }
        /*} else {
            sql.append("WHERE\n");
            domElement.appendChild(doc.createTextNode(sql.toString()));
            where.forEach(domElement::appendChild);
        }*/
        return versionParameter;
    }

    private org.w3c.dom.Element makeSimilarTo(FilterOption filter, Document doc, int tIdx, boolean isCaseIndependentLike) {
        org.w3c.dom.Element e = doc.createElement("if");
        e.setAttribute("test", filter.parameter + " != null");
        if (isCaseIndependentLike) {
            e.appendChild(doc.createTextNode("AND lower(t" + tIdx + "." +
                    filter.property.getColumnRef() + ")" + " SIMILAR TO (lower(#{" + filter.parameter +
                    ",javaType=java.lang.String,jdbcType=VARCHAR}))\n"));
        } else {
            e.appendChild(doc.createTextNode("AND t" + tIdx + "." +
                    filter.property.getColumnRef() + " SIMILAR TO (#{" + filter.parameter +
                    ",javaType=java.lang.String,jdbcType=VARCHAR})\n"));
        }
        return e;

    }


    private void addOrder(VariableElement order, org.w3c.dom.Element dynSql) {
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

    private void addRowBounds(DDataMethodBuilder.DDataMethodParameter rbParameter, org.w3c.dom.Element dynSql) {
        org.w3c.dom.Element ifElt = (org.w3c.dom.Element)
                dynSql.appendChild(dynSql.getOwnerDocument().createElement("if"));
        ifElt.setAttribute("test", rbParameter.name + " != null");
        ifElt.appendChild(dynSql.getOwnerDocument().createTextNode(
                "\nOFFSET #{" + rbParameter.name + ".offset,javaType=int,jdbcType=INTEGER}" +
                        "\nLIMIT #{" + rbParameter.name + ".limit,javaType=int,jdbcType=INTEGER}"));
    }

    private void addJoins(Collection<MappedTable> joins, StringBuilder sql, FilterOption versionParameter) {
        joins.stream()
                .filter(MappedTable::useInFieldsListOrFilters)
                .sorted(Comparator.comparingInt(t -> t.tableIndex))
                .forEach(join -> {
                    Mapping joinMap = builder.mappings.get(
                            join.property.dataBean.interfaceType.toString() + "." + join.property.name);
                    if (joinMap == null) return;

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
                    if (leftBean.versionalType != null) {
                        if (versionParameter == null || versionParameter.property == null) {
                            if (thisBean.versionalType == null)
                                throw new RuntimeException("try to join versional bean '" +
                                        leftBean.name + "' to non-versional '" +
                                        thisBean.name + "' without option");
                            else if (!environment.getTypeUtils().isSameType(thisBean.versionalType, leftBean.versionalType))
                                throw new RuntimeException("try to join versional beans [" +
                                        leftBean.name + "," + thisBean.name +
                                        "] with different version types");
                        }
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
                            if (versionParameter != null)
                                parameter = buildSqlParameter(versionParameter);
                            else
                                parameter = "tt.t";
                            sql
                                    .append("\n   AND t").append(join.tableIndex).append('.')
                                    .append(leftVersionFrom.columnName).append(" <= ").append(parameter)
                                    .append("\n   AND (t").append(join.tableIndex).append('.')
                                    .append(leftVersionTo.columnName).append(" > ").append(parameter)
                                    .append(" OR t").append(join.tableIndex).append('.')
                                    .append(leftVersionTo.columnName).append(" IS NULL)");
                        }
                    }

                    DataBeanPropertyBuilder discriminantProperty = leftBean.discriminatorProperty;
                    if (discriminantProperty != null) {
                        String dval = leftBean.discriminatorValue;
                        if (dval != null && dval.trim().length() > 0) {
                            sql.append("\n AND t").append(join.tableIndex).append('.').append('"')
                                    .append(discriminantProperty.columnName).append("\" = ");
                            if ("java.lang.String".equals(discriminantProperty.type.toString()))
                                sql.append('\'').append(dval).append('\'');
                            else
                                sql.append(dval);
                        }
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
            return beanProperty.getColumnWriter(jdbcTypeParameterFor(beanProperty.name, mappedType));
        } else
            return beanProperty.getColumnWriter(jdbcTypeParameterFor(beanProperty.name, beanProperty.type));
    }

    private String buildSqlParameter(DDataMapBuilder.FilterOption option) {
        return option.property == null ? "UNKNOWN_" : option.property.getColumnWriter(jdbcTypeParameterFor(option.parameter, option.property.type));
    }

    private String jdbcTypeParameterFor(String parameter, TypeMirror type) {
        return jdbcTypeParameterFor(parameter, type, false);
    }

    private String jdbcTypeParameterFor(String parameter, TypeMirror type, boolean forLazyLoadSelect) {
        String s = builder.jdbcTypeFor(type);
        StringBuilder p = new StringBuilder("#{");
        p.append(parameter);
        if (type.getKind() != TypeKind.ARRAY)
            p.append(", javaType=").append(type.getKind().isPrimitive() ?
                            environment.getTypeUtils().boxedClass((PrimitiveType) type) : (
                            forLazyLoadSelect ? convertTime(type, s) : type
                    )
            );
        if (s.length() > 0) p.append(", jdbcType=").append(s);
        return p.append('}').toString();
    }

    private String convertTime(TypeMirror type, String s) {
        if ("TIMESTAMP".equals(s)) return "java.sql.Timestamp";
        if ("DATE".equals(s)) return "java.sql.Date";
        if ("TIME".equals(s)) return "java.sql.Time";
        return type.toString();
    }

    private void addManagedBeanToFrom(StringBuilder sql, MappedTable mappedTable, FetchOptions fetchOptions) {
        String r = mappedTable.mappedBean.properties.values().stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
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

        map.setVersionalProperty(properties.iterator().next().dataBean.versionalType);

        properties.stream()
                .filter(DataBeanPropertyBuilder::notIgnored)
                .filter(p -> !(p.isId || p.isCollectionOrMap()))
                .filter(fetchOptions::filterIgnored)
                .filter(this::notManagedBean)
                //.filter(p -> !builder.beansByInterface.containsKey(p.type.toString()))
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
            lazy = fetchOptions.fetchType != DDataFetchType.EAGER || (mapping != null && mapping.alwaysLazy);
        } else {
            lazy = fetchOptions.fetchType == DDataFetchType.LAZY || (mapping != null && mapping.alwaysLazy);
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
                columnsMapping.append(mapping.stream()
                        .filter(m -> m.property.notIgnored())
                        .map(m -> m.key + "=" + (mappedTable.mappedFromTableIndex == 0 ? "" :
                                "t" + mappedTable.mappedFromTableIndex + "_") +
                                m.property.columnName)
                        .collect(Collectors.joining(",")));
                if (mappedBean.versionalType != null &&
                        environment.getTypeUtils().isSameType(thisBean.versionalType, mappedBean.versionalType)) {
                    if (thisVersionFrom != null && mappedVersionFrom != null) {
                        columnsMapping.append(',').append(mappedVersionFrom.name).append("=").append("dDataBeanActualAt_");
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

                    if (mappedBean.versionalType != null) ll.appendChild(doc.createTextNode(
                            thisVersionFrom != null && mappedVersionFrom != null ?
                                    ("\nWITH tt AS (SELECT CAST(#{" + mappedVersionFrom.name + "} AS TIMESTAMP) AS t)\n") :
                                    "\nWITH tt AS (SELECT NULL AS t)\n"
                    ));

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
                                        jdbcTypeParameterFor(m.key, propType, true));
                    }).collect(Collectors.joining(" AND "))).append("\n");

                    if (mappedBean.versionalType != null &&
                            environment.getTypeUtils().isSameType(thisBean.versionalType, mappedBean.versionalType))
                        if (thisVersionFrom != null && mappedVersionFrom != null && mappedVersionTo != null) {
                            sql.append("   AND t0.\"").append(mappedVersionFrom.columnName).append("\" <= tt.t")
                                    .append("\n   AND (t0.\"")
                                    .append(mappedVersionTo.columnName).append("\" > tt.t")
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
            int nextIndex = mappedTables.size() + 1;
            MappedTable mapped2Table = mappedTables.stream()
                    // fetchOptions & filters equals in call hierarchy
                    .filter(mt -> mappedTable.tableIndex == mt.mappedFromTableIndex
                            && mt.mappedBean == mapped2Bean && mt.property == mappedBean)
                    .findAny()
                    .orElse(new MappedTable(
                            mappedTable.tableIndex, nextIndex,
                            mappedBean, mapped2Bean, fetchOptions, filters)
                    );

            if (mapped2Table.notSingleSmallDictionaryValue()) {
                if (mapped2Table.tableIndex == nextIndex) mappedTables.add(mapped2Table);

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
                    fetchOptions.order.keySet().stream()
                            .map(o -> o.getColumnRef() + " " + fetchOptions.order.get(o))
                            .collect(Collectors.joining(", "))));
        }
    }

    class FilterOption {
        final DDataFilterOption option;
        final DataBeanPropertyBuilder property;
        final String parameter;
        final DataBeanPropertyBuilder mappedBy;
        final TypeMirror variableType;
        final String enumName;

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

                String key = filterProps.keySet().stream()
                        .filter(k -> "value".equals(k.getSimpleName().toString()))
                        .findAny()
                        .map(k -> filterProps.get(k).getValue().toString())
                        .orElse(null);

                DataBeanPropertyBuilder localProperty = key == null ? null : (
                        "VERSION_".equals(key) ?
                                bean.properties.values().stream()
                                        .filter(p -> p.isVersionFrom)
                                        .findAny()
                                        .orElse(null) :
                                bean.properties.values().stream()
                                        .filter(p -> p.enumName.equals(key))
                                        .findAny()
                                        .orElse(null));

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

                enumName = key;
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
                enumName = null;
            }
        }
    }

    private class FetchOptions {
        final DDataFetchType fetchType;
        final List<DataBeanPropertyBuilder> ignore;
        final List<DataBeanPropertyBuilder> exclusively;
        final int eagerTrunkLevel;
        final boolean truncateLazy;
        final Map<DataBeanPropertyBuilder, String> order;

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

            Object onlyObj = fetchProps.keySet().stream()
                    .filter(k -> "exclusively".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue())
                    .orElse(null);
            final List<Object> onlyList = (onlyObj != null && onlyObj instanceof List) ?
                    (List) onlyObj : Collections.emptyList();
            exclusively = onlyList.stream()
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

            order = new HashMap<>();
            Object orderObj = fetchProps.keySet().stream()
                    .filter(k -> "forwardOrder".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue())
                    .orElse(null);
            List<Object> orderList = (orderObj != null && orderObj instanceof List) ?
                    (List) orderObj : Collections.emptyList();
            orderList.stream()
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
                    .forEach(a -> order.put(a, "ASC"));

            orderObj = fetchProps.keySet().stream()
                    .filter(k -> "backwardOrder".equals(k.getSimpleName().toString()))
                    .findAny()
                    .map(k -> fetchProps.get(k).getValue())
                    .orElse(null);
            orderList = (orderObj != null && orderObj instanceof List) ?
                    (List) orderObj : Collections.emptyList();
            orderList.stream()
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
                    .forEach(a -> order.put(a, "DESC"));

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
            ignore = Collections.emptyList();
            exclusively = Collections.emptyList();
            eagerTrunkLevel = trunkLevel;
            truncateLazy = false;
            order = Collections.emptyMap();
        }

        private FetchOptions(FetchOptions template, DDataFetchType fetchType) {
            this.fetchType = fetchType == null ? DDataFetchType.COLLECTIONS_ARE_LAZY : fetchType;
            this.ignore = template.ignore;
            this.exclusively = template.exclusively;
            this.eagerTrunkLevel = template.eagerTrunkLevel;
            this.truncateLazy = template.truncateLazy;
            this.order = template.order;
        }

        boolean isIgnored(DataBeanPropertyBuilder property) {
            return this.exclusively.isEmpty() ?
                    this.ignore.contains(property) :
                    !this.exclusively.contains(property);
        }

        boolean filterIgnored(DataBeanPropertyBuilder property) {
            return !isIgnored(property);
        }

        boolean filter4ResultMap(DataBeanPropertyBuilder property) {
            return !(this.fetchType == DDataFetchType.NO ||
                    isIgnored(property) ||
                    (this.fetchType == DDataFetchType.COLLECTIONS_ARE_NO && property.isCollectionOrMap())
            );
        }

        boolean filter4FieldsList(DataBeanPropertyBuilder property) {
            return !(this.fetchType == DDataFetchType.NO ||
                    isIgnored(property) ||
                    ((
                            this.fetchType == DDataFetchType.COLLECTIONS_ARE_LAZY ||
                                    this.fetchType == DDataFetchType.COLLECTIONS_ARE_NO
                    ) && property.isCollectionOrMap()) ||
                    (this.fetchType == DDataFetchType.LAZY &&
                            builder.beansByInterface.containsKey(property.mappedType.toString())
                    )
            );
        }

        FetchOptions withLazy() {
            return new FetchOptions(this, DDataFetchType.LAZY);
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

        boolean useInFilters() {
            return useInFilters;
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
