package org.docero.data.processor;

import org.apache.ibatis.session.RowBounds;
import org.docero.data.utils.RowCounter;
import org.docero.data.DDataVersionalBean;
import org.docero.data.DDataVersionalRepository;
import org.docero.data.EnableDDataConfiguration;
import org.docero.dgen.processor.DGenClass;

import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.*;

class DDataBuilder {
    final ProcessingEnvironment environment;
    final HashMap<String, DataBeanBuilder> beansByInterface = new HashMap<>();
    final ArrayList<DataBeanBuilder> prototypesToGenerate = new ArrayList<>();
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
    final TypeMirror rowBoundsType;
    //For add SELECT COUNT(*) method
    final TypeMirror rowCounterType;
    final ArrayList<DataBeanBuilder> unimplementedBeans = new ArrayList<>();
    String basePackage = "org.docero.data";
    String componentName = "dData";

    DDataBuilder(ProcessingEnvironment environment) {
        this.environment = environment;
        TypeElement sqlSDS = environment.getElementUtils()
                .getTypeElement("org.mybatis.spring.support.SqlSessionDaoSupport");
        TypeElement dS = environment.getElementUtils()
                .getTypeElement("org.springframework.dao.support.DaoSupport");
        spring = dS != null && sqlSDS != null && sqlSDS.getKind() == ElementKind.CLASS;
        temporalType = environment.getElementUtils().getTypeElement(java.time.temporal.Temporal.class.getCanonicalName()).asType();
        oldDateType = environment.getElementUtils().getTypeElement(java.util.Date.class.getCanonicalName()).asType();
        useSpringCache = environment.getElementUtils().getTypeElement(
                "org.springframework.cache.annotation.Cacheable") != null;

        collectionType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement(java.util.Collection.class.getCanonicalName()).asType()
        );
        mapType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement(java.util.Map.class.getCanonicalName()).asType()
        );
        versionalBeanType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement(DDataVersionalBean.class.getCanonicalName()).asType()
        );
        versionalRepositoryType = environment.getTypeUtils().erasure(
                environment.getElementUtils().getTypeElement(DDataVersionalRepository.class.getCanonicalName()).asType()
        );
        voidType = environment.getElementUtils().getTypeElement(Void.class.getCanonicalName()).asType();
        stringType = environment.getElementUtils().getTypeElement(String.class.getCanonicalName()).asType();
        rowBoundsType = environment.getElementUtils().getTypeElement(RowBounds.class.getCanonicalName()).asType();
        rowCounterType = environment.getElementUtils().getTypeElement(RowCounter.class.getCanonicalName()).asType();
    }

    void logError(String message) {
        environment.getMessager().printMessage(Diagnostic.Kind.ERROR, message);
        System.err.println("DDataProcessor Exception: " + message);
    }

    void checkInterface(TypeElement beanElement, boolean isPrototype) {
        try {
            String typeName = beanElement.asType().toString();
            packages.add(typeName.substring(0, typeName.lastIndexOf('.')));
            DataBeanBuilder value = isPrototype ?
                    DataBeanBuilder.buildPrototype(beanElement, this) :
                    DataBeanBuilder.buildEntity(beanElement, this);
            if (isPrototype) prototypesToGenerate.add(value);
            else beansByInterface.put(value.interfaceType.toString(), value);
        } catch (Exception e) {
            logError("error processing " + beanElement);
            throw e;
        }
    }

    void checkDGenInterface(TypeElement beanElement) {
        DGenClass dGen = DGenClass.readInterface(beanElement);
        dGenInterface.put(dGen.getTargetClassName(), dGen);
    }

    void checkRepository(TypeElement repositoryElement) throws Exception {
        try {
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
        } catch (Exception e) {
            logError("error processing " + repositoryElement);
            throw e;
        }
    }

    void generateBeansAnnotations() throws IOException {
        ArrayList<DataBeanBuilder> beans = new ArrayList<>(beansByInterface.values());
        beans.addAll(prototypesToGenerate);
        for (DataBeanBuilder bean : beans)
            try {
                bean.buildAnnotationsAndEnums(environment, beansByInterface);
            } catch (Exception e) {
                logError("error build annotations for " + bean.interfaceType);
                throw e;
            }
    }

    void generateRepositoriesAnnotations() throws IOException {
        for (DataRepositoryBuilder repository : repositories)
            try {
                repository.buildAnnotations(environment, spring);
            } catch (Exception e) {
                logError("error build annotations for " + repository.repositoryInterface);
                throw e;
            }
    }

    void generatePrototypes() throws IOException {
        for (DataBeanBuilder bean : prototypesToGenerate)
            bean.buildImplementation(environment);
        buildDataReferenceEnums(prototypesToGenerate);
    }

    void generateImplementation() throws IOException {
        if (beansByInterface.isEmpty()) return;

        try (JavaClassWriter cf = new JavaClassWriter(environment, basePackage + ".AbstractBean")) {
            cf.println("package " + basePackage + ";");
            cf.println("import org.docero.data.DData;");
            cf.startBlock("/*");
            cf.println("Class generated by docero-data processor.");
            cf.endBlock("*/");
            cf.println("@javax.xml.bind.annotation.XmlTransient");
            cf.startBlock("public abstract class AbstractBean<T extends java.io.Serializable> " +
                    "implements org.docero.data.DDataComparable<T> {");
            cf.startBlock("protected <P extends java.io.Serializable> P cached(Class<P> type, java.io.Serializable key) {");
            cf.println("return DData.cache(type, key);");
            cf.endBlock("}");
            cf.println("");
            cf.startBlock("protected <P extends java.io.Serializable> P remote(Class<P> type, java.lang.String func, java.io.Serializable... key) {");
            cf.println("return key == null || key.length == 0 || (key.length == 1 && key[0] == null) ? null : DData.remote(type, func, key);");
            cf.endBlock("}");

            cf.println("");
            cf.startBlock("protected int compare(Object o1, Object o2) {");
            cf.println("if (o1 == null) return o2 == null ? 0 : -1;");
            cf.println("if (o2 == null) return 1;");
            cf.println("if (o1 instanceof java.lang.Comparable)");
            cf.println("   return ((java.lang.Comparable) o1).compareTo(o2);");
            cf.println("else");
            cf.println("   return 0;");
            cf.endBlock("}");

            cf.endBlock("}");
        }
        checkAbstractRepositoryForPackage(basePackage);

        for (DataBeanBuilder bean : beansByInterface.values()) bean.buildImplementation(environment);
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

        if ("Ljava.lang.Byte".equals(s) || "java.lang.Byte[]".equals(s) || "byte[]".equals(s)
                ) return "BINARY";

        return "";
    }

    void buildDataReferenceEnums(Collection<DataBeanBuilder> col) throws IOException {
        for (DataBeanBuilder dataBeanBuilder : col)
            try {
                dataBeanBuilder.buildDataReferenceEnum(environment, mappings);
            } catch (Exception e) {
                logError("error build enums for " + dataBeanBuilder.interfaceType);
                throw e;
            }
    }

    List<TypeMirror> readBeansFromBeanElement(TypeElement repositoryElement) {
        ArrayList<TypeMirror> beans = new ArrayList<>();
        try {
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
        } catch (Exception e) {
            logError("error build beans from DDataRep annotation on " + repositoryElement);
            throw e;
        }
        return beans;
    }

    void checkBasePackage(RoundEnvironment roundEnv) {
        Set<? extends Element> elts = roundEnv.getElementsAnnotatedWith(EnableDDataConfiguration.class);
        if (!elts.isEmpty()) {
            Element elt = elts.iterator().next();
            EnableDDataConfiguration dc = elt.getAnnotation(EnableDDataConfiguration.class);
            if (dc.packageName().length() > 0) {
                this.basePackage = dc.packageName();
            } else {
                this.basePackage = elt.getAnnotationMirrors().stream()
                        .filter(m -> m.getAnnotationType().toString().endsWith("EnableDDataConfiguration"))
                        .findAny()
                        .map(m -> m.getElementValues().entrySet().stream()
                                .filter(e -> e.getKey().toString().equals("packageClass()"))
                                .findAny().map(e -> e.getValue().getValue().toString()).orElse("")
                        )
                        .map(cn -> cn.substring(0, cn.lastIndexOf('.')))
                        .orElse("org.docero.data");
            }
            this.componentName = dc.springComponentName().length() > 0 ? dc.springComponentName() : "dData";
        }
    }

    boolean isSimpleMappedType(TypeMirror returnType) {
        try {
            return returnType.getKind().isPrimitive() ||
                    returnType.toString().equals("byte[]") ||
                    returnType.toString().startsWith("java.lang.") ||
                    returnType.toString().startsWith("java.time.") ||
                    returnType.toString().startsWith("java.sql.") ||
                    "java.math.BigDecimal".equals(returnType.toString()) ||
                    "java.math.BigInteger".equals(returnType.toString()) ||
                    "java.util.UUID".equals(returnType.toString());
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private HashSet<String> abstractRepositoryCreated = new HashSet<>();

    void checkAbstractRepositoryForPackage(String beanPkg) throws IOException {
        if(!abstractRepositoryCreated.contains(beanPkg)) {
            try (JavaClassWriter cf = new JavaClassWriter(environment, beanPkg + ".AbstractRepository")) {
                cf.println("package " + beanPkg + ";");
                cf.println("import org.docero.data.DData;");
                cf.startBlock("/*");
                cf.println("Class generated by docero-data processor.");
                cf.endBlock("*/");
                cf.startBlock("public abstract class AbstractRepository<T extends java.io.Serializable, C extends java.io.Serializable> " +
                        (spring ? " extends org.docero.data.AbstractSpringRepository<T,C>" : " extends org.docero.data.AbstractRepository<T,C>") +
                        " {");
                cf.endBlock("}");
            }
            abstractRepositoryCreated.add(beanPkg);
        }
    }
}
