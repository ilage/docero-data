package org.docero.data.processor;

import org.docero.data.DDataBean;
import org.docero.data.DDataRep;
import org.docero.data.remote.DDataPrototype;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import java.util.HashMap;
import java.util.Set;

/**
 * Процессор анотаций.
 * <p>Подключение в Maven: &lt;annotationProcessor&gt;org.docero.data.processor.DDataProcessor&lt;/annotationProcessor&gt;</p>
 * Created by i.vasyashin on 01.09.2017.
 */
@SupportedAnnotationTypes({
        "org.docero.data.remote.DDataPrototype",
        "org.docero.data.DDataBean",
        "org.docero.data.DDataRep"
})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class DDataProcessor extends AbstractProcessor {
    private DDataBuilder builder;

    private enum Stage {
        STEP1_ENUM_GEN,
        STEP2_BEANS_GEN,
        STEP3_MAPS_GEN,
        STEP_END
    }

    private Stage stage = Stage.STEP1_ENUM_GEN;

    @Override
    public void init(ProcessingEnvironment environment) {
        super.init(environment);
        builder = new DDataBuilder(environment);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            Set<? extends Element> repositories;
            switch (stage) {
                case STEP1_ENUM_GEN:
                    builder.checkBasePackage(roundEnv);

                    Set<? extends Element> prototypes = roundEnv.getElementsAnnotatedWith(DDataPrototype.class);
                    for (Element beanElement : prototypes)
                        if (beanElement.getEnclosingElement().getKind() == ElementKind.PACKAGE)
                            builder.checkInterface((TypeElement) beanElement, true);

                    Set<? extends Element> entities = roundEnv.getElementsAnnotatedWith(DDataBean.class);
                    for (Element beanElement : entities)
                        if (beanElement.getEnclosingElement().getKind() != ElementKind.PACKAGE)
                            builder.checkDGenInterface((TypeElement) beanElement);
                    for (Element beanElement : entities)
                        if (beanElement.getEnclosingElement().getKind() == ElementKind.PACKAGE)
                            builder.checkInterface((TypeElement) beanElement, false);

                    builder.generateBeansAnnotations();

                    repositories = roundEnv.getElementsAnnotatedWith(DDataRep.class);
                    for (Element repositoryElement : repositories)
                        if (repositoryElement.getKind() == ElementKind.INTERFACE)
                            builder.checkRepository((TypeElement) repositoryElement);
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
                        builder.buildDataReferenceEnums(builder.prototypesToGenerate);
                        builder.buildDataReferenceEnums(builder.beansByInterface.values());
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
            if (element.getKind() == ElementKind.METHOD && !(
                    element.getModifiers().contains(Modifier.DEFAULT) || element.getModifiers().contains(Modifier.STATIC)
            ))
                try {
                    ExecutableElement method = (ExecutableElement) element;
                    String mappingKey = beanElement.asType().toString() + "." +
                            propertyName4Method(method.getSimpleName().toString());
                    for (AnnotationMirror annotationMirror : method.getAnnotationMirrors())
                        if (annotationMirror.getAnnotationType().toString().contains("_Map_")) {
                            Mapping mapping = new Mapping(annotationMirror, bean);
                            if (!mapping.mappedProperties.isEmpty())
                                builder.mappings.put(mappingKey, mapping);
                            break;
                        }
                } catch (Exception e) {
                    builder.logError("can't build mappings for " + element + " in " + beanElement);
                    throw e;
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
