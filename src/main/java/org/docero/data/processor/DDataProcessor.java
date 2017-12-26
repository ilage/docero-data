package org.docero.data.processor;

import org.docero.data.DDataBean;
import org.docero.data.DDataRep;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeMirror;
import java.util.HashMap;
import java.util.Set;

/**
 * Процессор анотаций.
 * <p>Подключение в Maven: &lt;annotationProcessor&gt;org.docero.data.processor.DDataProcessor&lt;/annotationProcessor&gt;</p>
 * Created by i.vasyashin on 01.09.2017.
 */
@SupportedAnnotationTypes({
        "org.docero.data.DDataBean",
        "org.docero.data.DDataRep"
})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class DDataProcessor extends AbstractProcessor {
    private TypeMirror collectionType;
    private TypeMirror mapType;
    private DDataBuilder builder;
    private TypeMirror versionalBeanType;
    private TypeMirror versionalRepositoryType;

    private enum Stage {
        STEP1_ENUM_GEN,
        STEP1p_REPOSITORIES_GEN,
        STEP2_BEANS_GEN,
        STEP3_MAPS_GEN,
        STEP_END
    }

    private Stage stage = Stage.STEP1_ENUM_GEN;

    @Override
    public void init(ProcessingEnvironment environment) {
        super.init(environment);
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
        builder = new DDataBuilder(environment);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            Set<? extends Element> repositories;
            switch (stage) {
                case STEP1_ENUM_GEN:
                    Set<? extends Element> entities = roundEnv.getElementsAnnotatedWith(DDataBean.class);
                    for (Element beanElement : entities)
                        if (beanElement.getEnclosingElement().getKind() != ElementKind.PACKAGE)
                            builder.checkDGenInterface((TypeElement) beanElement);
                    for (Element beanElement : entities)
                        if (beanElement.getEnclosingElement().getKind() == ElementKind.PACKAGE)
                            builder.checkInterface((TypeElement) beanElement, collectionType, mapType, versionalBeanType);

                    builder.generateBeansAnnotations();

                    repositories = roundEnv.getElementsAnnotatedWith(DDataRep.class);
                    for (Element repositoryElement : repositories)
                        if (repositoryElement.getKind() == ElementKind.INTERFACE)
                            builder.checkRepository((TypeElement) repositoryElement, versionalRepositoryType);
                    builder.generateRepositoriesAnnotations();

                    stage = Stage.STEP2_BEANS_GEN;
                    break;
                case STEP2_BEANS_GEN:
                    HashMap<String, TypeElement> pkgClasses = listClasses();
                    for (DataBeanBuilder bean : builder.beansByInterface.values()) {
                        buildMappingFor(pkgClasses.get(bean.interfaceType.toString()), bean);
                    }

                    builder.generateImplementation();
                    builder.generateDdata();

                    stage = Stage.STEP3_MAPS_GEN;
                    break;
                case STEP3_MAPS_GEN:
                    if (new DDataMapBuilder(builder, this.processingEnv).build(listClasses())) {
                        builder.buildDataReferenceEnums();
                        stage = Stage.STEP_END;
                    }
                    break;
                case STEP_END:
                    break;
                default:
            }
        } catch (Exception e) {
            //processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,e.getMessage());
            e.printStackTrace();
            throw new Error(e);
        }
        return false;
    }

    private HashMap<String, TypeElement> listClasses() {
        HashMap<String, TypeElement> pkgClasses = new HashMap<>();
        for (String aPackage : builder.packages) {
            PackageElement pkg = builder.environment.getElementUtils().getPackageElement(aPackage);
            for (Element element : pkg.getEnclosedElements()) {
                pkgClasses.put(element.asType().toString(), (TypeElement) element);
            }
        }
        return pkgClasses;
    }

    private void buildMappingFor(TypeElement beanElement, DataBeanBuilder bean) {
        for (Element element : builder.environment.getElementUtils().getAllMembers(beanElement)) {
            if (element.getKind() == ElementKind.METHOD) {
                ExecutableElement method = (ExecutableElement) element;
                String mappingKey = beanElement.asType().toString() + "." +
                        propertyName4Method(method.getSimpleName().toString());
                Mapping mapping = null;
                for (AnnotationMirror annotationMirror : method.getAnnotationMirrors())
                    if (annotationMirror.getAnnotationType().toString().contains("_Map_")) {
                        mapping = new Mapping(annotationMirror, bean);
                        if (!mapping.mappedProperties.isEmpty())
                            builder.mappings.put(mappingKey, mapping);

                        /*System.out.println(annotationMirror.getAnnotationType() + " tail:");
                        System.out.println(mappingKey + "\n   " +
                                beanElement.asType().toString() + "." + mapping.property.name +
                                (mapping.manyToOne ? " <- " : " -> ") +
                                mapping.mappedProperty.dataBean.interfaceType + "." +
                                mapping.mappedProperty.name);*/
                        break;
                    }

                if (mapping == null && method.getReturnType() != null) {
                    DataBeanBuilder mappedBean = builder.beansByInterface
                            .get(builder.environment.getTypeUtils().erasure(
                                    method.getReturnType()
                            ).toString());
                    if (mappedBean != null) {
                        // надо ли это?
                        DataBeanPropertyBuilder property = propertyName4Method(bean, method);
                        mapping = new Mapping(property, mappedBean);
                        if (!mapping.mappedProperties.isEmpty())
                            builder.mappings.put(mappingKey, mapping);
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
}
