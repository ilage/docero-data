package org.docero.data.processor;

import org.docero.data.DDataBean;
import org.docero.data.DDataFetch;
import org.docero.data.DDataFilter;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeMirror;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class DDataMapBuilder {
    static void build(DDataBuilder builder, ProcessingEnvironment environment, TypeMirror collectionType) {
        if (builder.beansByInterface.isEmpty()) return;

        HashMap<String, TypeElement> pkgClasses = new HashMap<>();
        for (String aPackage : builder.packages) {
            PackageElement pkg = environment.getElementUtils().getPackageElement(aPackage);
            for (Element element : pkg.getEnclosedElements()) {
                pkgClasses.put(element.asType().toString(), (TypeElement) element);
            }
        }

        for (TypeElement typeElement : pkgClasses.values()) {
            DDataBean dataBean = typeElement.getAnnotation(DDataBean.class);
            if (dataBean != null) {
                //TypeElement enumElement = pkgClasses.get(typeElement.getSimpleName() + "_");
                //if (enumElement != null) {
                DataBeanBuilder beanBuilder = builder.beansByInterface.get(typeElement.asType().toString());
                for (Element element : typeElement.getEnclosedElements()) {
                    if (element.getKind() == ElementKind.METHOD) {
                        AnnotationMirror mapInterface = findAnnotation(element, "_Map_");
                        if (mapInterface != null) {
                            Map<? extends ExecutableElement, ? extends AnnotationValue> map =
                                    environment.getElementUtils().getElementValuesWithDefaults(mapInterface);
                            DataBeanPropertyBuilder localField = null;
                            DataBeanPropertyBuilder foreignField = null;
                            for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> pair : map.entrySet()) {
                                String val = pair.getValue().getValue().toString();
                                if (val.length() > 0) {
                                    int valIdx = val.lastIndexOf('.');
                                    if (valIdx < 0) {
                                        localField = beanBuilder.properties.get(val);
                                    } else {
                                        String searchVal = val.substring(0, valIdx - 1);
                                        DataBeanBuilder subBeabB = builder.beansByInterface.get(searchVal);
                                        foreignField = subBeabB.properties.get(val.substring(valIdx + 1));
                                    }
                                }
                            }
                            if (localField != null && foreignField != null) {
                                //foreignField.dataBean;
                            }
                            //System.out.println(typeElement.getSimpleName() + "." + element.getSimpleName());
                        }
                    }
                }
                //}
            }
        }


        for (DataRepositoryBuilder repository : builder.repositories) {
            TypeElement repositoryElement = pkgClasses.get(repository.repositoryInterface.toString());
            if (repositoryElement == null) {
                System.out.println("dynamical (" + repository.repositoryInterface.toString() +
                        ") for " + repository.forInterfaceName());

            } else {
                System.out.println("" + repositoryElement.asType() + " is for " + repository.forInterfaceName());

                for (Element methodElement : repositoryElement.getEnclosedElements()) {
                    DDataFetch fetchType = methodElement.getAnnotation(DDataFetch.class);
                    for (VariableElement variableElement : ((ExecutableElement) methodElement).getParameters()) {
                        DDataFilter dataFilter = variableElement.getAnnotation(DDataFilter.class);
                        if (dataFilter != null) {

                        } else {
                            Optional<? extends AnnotationMirror> filterOpt = variableElement.getAnnotationMirrors().stream()
                                    .filter(a -> a.toString().indexOf("_Filter_") > 0)
                                    .findAny();
                            if (filterOpt.isPresent()) {
                                AnnotationMirror filterMirror = filterOpt.get();
                                Map<? extends ExecutableElement, ? extends AnnotationValue> map = environment.getElementUtils().getElementValuesWithDefaults(filterMirror);
                                System.out.println(
                                        "" + methodElement.getSimpleName() + "(" + variableElement + ") " + map);
                            }
                        }
                    }
                }
            }
        }
    }

    private static AnnotationMirror findAnnotation(Element element, String suffix) {
        for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {
            if (annotationMirror.getAnnotationType().toString().indexOf(suffix) > 0)
                return annotationMirror;
        }
        return null;
    }
}
