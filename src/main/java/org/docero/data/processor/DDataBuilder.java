package org.docero.data.processor;

import org.docero.dgen.processor.DGenClass;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

class DDataBuilder {
    final ProcessingEnvironment environment;
    final HashMap<String, DataBeanBuilder> beansByInterface = new HashMap<>();
    final HashMap<String, DGenClass> dGenInterface = new HashMap<>();
    final ArrayList<DataRepositoryBuilder> repositories = new ArrayList<>();
    final ArrayList<BatchRepositoryBuilder> batchRepositories = new ArrayList<>();
    final HashMap<String, DataRepositoryBuilder> repositoriesByBean = new HashMap<>();
    final HashSet<String> packages = new HashSet<>();
    final boolean spring;
    final HashMap<String, Mapping> mappings = new HashMap<>();
    final TypeMirror temporalType;
    final TypeMirror oldDateType;
    final boolean useSpringCache;
    final TypeMirror collectionType;
    final TypeMirror mapType;
    final TypeMirror versionalBeanType;
    final TypeMirror versionalRepositoryType;
    final TypeMirror voidType;
    final TypeMirror stringType;
    final ArrayList<DataBeanBuilder> unimplementedBeans = new ArrayList<>();

    DDataBuilder(ProcessingEnvironment environment) {
        this.environment = environment;
        TypeElement sqlSDS = environment.getElementUtils()
                .getTypeElement("org.mybatis.spring.support.SqlSessionDaoSupport");
        TypeElement dS = environment.getElementUtils()
                .getTypeElement("org.springframework.dao.support.DaoSupport");
        spring = dS != null && sqlSDS != null && sqlSDS.getKind() == ElementKind.CLASS;
        temporalType = environment.getElementUtils().getTypeElement("java.time.temporal.Temporal").asType();
        oldDateType = environment.getElementUtils().getTypeElement("java.util.Date").asType();
        useSpringCache = environment.getElementUtils().getTypeElement(
                "org.springframework.cache.annotation.Cacheable") != null;

        collectionType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement("java.util.Collection").asType()
        );
        mapType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement("java.util.Map").asType()
        );
        versionalBeanType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement("org.docero.data.DDataVersionalBean").asType()
        );
        versionalRepositoryType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement("org.docero.data.DDataVersionalRepository").asType()
        );
        voidType = environment.getElementUtils().getTypeElement("java.lang.Void").asType();
        stringType = environment.getElementUtils().getTypeElement("java.lang.String").asType();
    }

    void checkInterface(TypeElement beanElement) {
        String typeName = beanElement.asType().toString();
        packages.add(typeName.substring(0, typeName.lastIndexOf('.')));
        DataBeanBuilder value = new DataBeanBuilder(beanElement, this);
        beansByInterface.put(value.interfaceType.toString(), value);
    }

    void checkDGenInterface(TypeElement beanElement) {
        DGenClass dGen = DGenClass.readInterface(beanElement);
        dGenInterface.put(dGen.getTargetClassName(), dGen);
    }

    void checkRepository(TypeElement repositoryElement, TypeMirror versionalType) throws Exception {
        String typeName = repositoryElement.asType().toString();
        packages.add(typeName.substring(0, typeName.lastIndexOf('.')));
        if (repositoryElement.getInterfaces().stream()
                .anyMatch(i -> i.toString().contains("org.docero.data.DDataBatchOpsRepository"))) {
            BatchRepositoryBuilder builder =
                    new BatchRepositoryBuilder(this, repositoryElement);
            batchRepositories.add(builder);
        } else {
            DataRepositoryBuilder builder =
                    new DataRepositoryBuilder(this, repositoryElement);
            repositoriesByBean.put(builder.forInterfaceName(), builder);
            repositories.add(builder);

            for (DataBeanBuilder beanBuilder : unimplementedBeans)
                beanBuilder.buildAnnotationsAndEnums(environment, beansByInterface);
            unimplementedBeans.clear();
        }
    }

    void generateBeansAnnotations() throws IOException {
        for (DataBeanBuilder bean : beansByInterface.values())
            bean.buildAnnotationsAndEnums(environment, beansByInterface);
    }

    void generateRepositoriesAnnotations() throws IOException {
        for (DataRepositoryBuilder repository : repositories)
            repository.buildAnnotations(environment, spring);
    }

    void generateImplementation() throws IOException {
        try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.AbstractBean")) {
            cf.println("package org.docero.data;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public abstract class AbstractBean {");
            cf.startBlock("protected <T extends java.io.Serializable> T cached(Class<T> type, java.io.Serializable key) {");
            cf.println("return DData.cache(type, key);");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("protected <T> int compare(T o1, T o2) {");
            cf.println("if (o1 == null) return o2 == null ? 0 : -1;");
            cf.println("if (o2 == null) return 1;");
            cf.println("if (o1 instanceof java.lang.Comparable)");
            cf.println("   return ((java.lang.Comparable) o1).compareTo(o2);");
            cf.println("else");
            cf.println("   return 0;");
            cf.endBlock("}");

            cf.endBlock("}");
        }
        try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.AbstractRepository")) {
            cf.println("package org.docero.data;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public abstract class AbstractRepository<T extends java.io.Serializable, C extends java.io.Serializable> " +
                    (spring ? " extends org.mybatis.spring.support.SqlSessionDaoSupport" : "") +
                    " implements DDataRepository<T,C> {");
            cf.startBlock("protected <T extends java.io.Serializable> void cache(T bean) {");
            cf.println("DData.cache(bean);");
            cf.endBlock("}");
            cf.println("");
            cf.startBlock("protected <T extends java.io.Serializable> void cache(java.util.Collection<T> beans) {");
            cf.println("DData.cache(beans);");
            cf.endBlock("}");

            cf.endBlock("}");
        }

        for (DataBeanBuilder bean : beansByInterface.values()) {
            bean.buildImplementation(environment);
        }
    }

    void generateDdata() throws IOException {
        for (DataBeanBuilder bean : beansByInterface.values()) {
            if (!repositoriesByBean.containsKey(bean.interfaceType.toString())) {
                DataRepositoryBuilder r = DataRepositoryBuilder.build(this, bean);
                repositoriesByBean.put(bean.interfaceType.toString(), r);
                repositories.add(r);
            }
        }

        for (BatchRepositoryBuilder batchRepository : batchRepositories) {
            batchRepository.generate();
        }

        try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.utils.DDataObjectFactory")) {
            cf.println("package org.docero.data.utils;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public class DDataObjectFactory extends DDataAbstractObjectFactory {");
            cf.startBlock("<T> Class<? extends T> getImplementation(Class<T> type) {");
            cf.println("return org.docero.data.DData.implementations.containsKey(type) ? " +
                    "(Class<? extends T>) org.docero.data.DData.implementations.get(type) : type;");
            cf.endBlock("}");
            cf.endBlock("}");
        }
        /*
            class DData - static methods for accessing library classes
        */
        try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.DData")) {
            cf.println("package org.docero.data;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.startBlock("public class DData {");

            cf.startBlock("static <T extends java.io.Serializable,C extends java.io.Serializable> " +
                    "DDataRepository<T,C> getRepository(Class<T> repositoryClass) {");
            for (String repositoryFor : repositoriesByBean.keySet()) {
                DataRepositoryBuilder repository = repositoriesByBean.get(repositoryFor);
                cf.println("if (repositoryClass == " +
                        repositoryFor + ".class) return (DDataRepository<T, C>) new " +
                        repository.daoClassName + "();");
            }
            cf.println("return null;");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("public static final java.util.Map<Class<?>,Class<?>> implementations = " +
                    "new java.util.HashMap<Class<?>,Class<?>>() {{");
            for (String interfaceName : beansByInterface.keySet()) {
                cf.println("this.put(" +
                        interfaceName + ".class," +
                        beansByInterface.get(interfaceName).getImplementationName() + ".class);");
            }
            cf.endBlock("}};");

            cf.println("");
            cf.startBlock("public static final String[] cacheNames = new String[] {");
            cf.println(beansByInterface.values().stream()
                    .filter(DataBeanBuilder::isDictionary)
                    .map(b -> "\"" + b.cacheMap + "\"")
                    .collect(Collectors.joining(",\n\t")));
            cf.endBlock("};");

            cf.println("");
            cf.println("private static org.docero.data.DDataDictionariesService dictionariesService = " +
                    "new org.docero.data.DDataDictionariesService();");
            cf.println("");
            cf.startBlock("public static org.apache.ibatis.reflection.factory.ObjectFactory getObjectFactory() {");
            cf.println("return new org.docero.data.utils.DDataObjectFactory();");
            cf.endBlock("}");
            cf.println("");
            cf.startBlock("static <T extends java.io.Serializable, C extends java.io.Serializable> void registerAsDictionary(DDataRepository<T, C> repository, Class<? extends T>... types) {");
            cf.println("for(Class<? extends T> type : types) dictionariesService.register(type, repository);");
            cf.endBlock("}");
            cf.println("");
            cf.startBlock("static <T extends java.io.Serializable> void cache(T bean) {");
            cf.println("dictionariesService.put(bean);");
            cf.endBlock("}");
            cf.println("");
            cf.startBlock("static <T extends java.io.Serializable> void cache(java.util.Collection<T> bean) {");
            cf.println("dictionariesService.put(bean);");
            cf.endBlock("}");
            cf.println("");
            cf.startBlock("static <T extends java.io.Serializable, C extends java.io.Serializable> T cache(Class<T> type, C key) {");
            cf.println("return dictionariesService.get(type,key);");
            cf.endBlock("}");

            if (environment.getElementUtils().getTypeElement("com.fasterxml.jackson.databind.JsonDeserializer") != null) {
                cf.println("");
                cf.startBlock("/**");
                cf.println("simple usage: DData.deserializers.forEach(builder::deserializerByType);");
                cf.println("<p>where builder is org.springframework.http.converter.json.Jackson2ObjectMapperBuilder</p>");
                cf.endBlock("*/");
                cf.startBlock("public static final java.util.Map<Class<?>,com.fasterxml.jackson.databind.JsonDeserializer<?>> deserializers = " +
                        "new java.util.HashMap<Class<?>,com.fasterxml.jackson.databind.JsonDeserializer<?>>() {{");

                for (String interfaceName : beansByInterface.keySet()) {
                    cf.startBlock("this.put(" +
                            interfaceName + ".class, new com.fasterxml.jackson.databind.JsonDeserializer<" +
                            interfaceName + ">() {");
                    cf.println("@Override");
                    cf.startBlock("public " + interfaceName + " deserialize(com.fasterxml.jackson.core.JsonParser p, " +
                            "com.fasterxml.jackson.databind.DeserializationContext ctxt) throws " +
                            "java.io.IOException, com.fasterxml.jackson.core.JsonProcessingException {");
                    cf.println("return ctxt.readValue(p, " +
                            beansByInterface.get(interfaceName).getImplementationName() +
                            ".class);");
                    cf.endBlock("}");
                    cf.endBlock("});");
                }

                cf.endBlock("}};");
            }

            cf.endBlock("}");
        }

        if (spring)
            try (JavaClassWriter cf = new JavaClassWriter(environment, "org.docero.data.DDataResources")) {
                cf.println("package org.docero.data;");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.startBlock("public class DDataResources {");
                cf.println("private java.util.List<org.springframework.core.io.Resource> list = new java.util.ArrayList<>();");
                cf.println("DDataResources() {}");

                cf.startBlock("public org.springframework.core.io.Resource[] asArray() {;");
                cf.println("return list.toArray(new org.springframework.core.io.Resource[list.size()]);");
                cf.endBlock("}");

                cf.startBlock("void add(org.springframework.core.io.Resource res) {");
                cf.println("list.add(res);");
                cf.endBlock("}");

                cf.endBlock("}");
            }
    }

    String jdbcTypeFor(TypeMirror type) {
        String s = type.toString();

        if (java.time.LocalDate.class.getCanonicalName().equals(s)
                ) return "DATE";

        if (java.time.LocalTime.class.getCanonicalName().equals(s)
                ) return "TIME";

        if (//not work environment.getTypeUtils().isSubtype(type, temporalType) ||
                environment.getTypeUtils().directSupertypes(type).stream()
                        .anyMatch(c -> c.toString().equals(temporalType.toString())) ||
                        environment.getTypeUtils().isSubtype(type, oldDateType)
                ) return "TIMESTAMP";

        if (String.class.getCanonicalName().equals(s))
            return "VARCHAR";

        if ("boolean".equals(s) ||
                java.lang.Boolean.class.getCanonicalName().equals(s)
                ) return "BOOLEAN";

        if ("short".equals(s) ||
                java.lang.Short.class.getCanonicalName().equals(s)
                ) return "SMALLINT";

        if ("int".equals(s) ||
                java.lang.Integer.class.getCanonicalName().equals(s)
                ) return "INTEGER";

        if ("long".equals(s) ||
                java.lang.Long.class.getCanonicalName().equals(s)
                ) return "BIGINT";

        if ("float".equals(s) ||
                java.lang.Float.class.getCanonicalName().equals(s)
                ) return "REAL";

        if ("double".equals(s) ||
                java.lang.Double.class.getCanonicalName().equals(s)
                ) return "DOUBLE";

        if (java.math.BigInteger.class.getCanonicalName().equals(s) ||
                java.math.BigDecimal.class.getCanonicalName().equals(s)
                ) return "NUMERIC";

        return "";
    }

    void buildDataReferenceEnums() throws IOException {
        for (DataBeanBuilder dataBeanBuilder : beansByInterface.values()) {
            dataBeanBuilder.buildDataReferenceEnum(environment, beansByInterface, mappings);
        }
    }

    List<TypeMirror> readBeansFromBeanElement(TypeElement repositoryElement) {
        ArrayList<TypeMirror> beans = new ArrayList<>();
        Optional<Object> beansOpt = repositoryElement.getAnnotationMirrors().stream()
                .filter(m -> m.getAnnotationType().toString().endsWith("DDataRep"))
                .findAny()
                .map(m -> m.getElementValues().entrySet().stream()
                        .filter(e -> e.getKey().toString().equals("beans()"))
                        .findAny().map(e -> e.getValue().getValue()).orElse(""));
        if (beansOpt.isPresent()) {
            List b = beansOpt.get() instanceof List ? (List) beansOpt.get() : Collections.singletonList(beansOpt.get());
            for (Object v : b) {
                String classValue = v.toString();
                if (classValue.length() > 0) beans.add(environment.getElementUtils()
                        .getTypeElement(classValue.substring(0, classValue.lastIndexOf('.'))).asType());
            }
        }
        return beans;
    }
}
