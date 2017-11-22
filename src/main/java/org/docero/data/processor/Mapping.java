package org.docero.data.processor;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

class Mapping {
    final List<DataBeanPropertyBuilder> properties = new ArrayList<>();
    final List<DataBeanPropertyBuilder> mappedProperties = new ArrayList<>();
    final boolean manyToOne;

    private final DDataBuilder builder;

    Mapping(AnnotationMirror annotationMirror, DataBeanBuilder bean) {
        builder = bean.rootBuilder;
        Map<? extends ExecutableElement, ? extends AnnotationValue> map =
                builder.environment.getElementUtils().getElementValuesWithDefaults(annotationMirror);

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
                        .forEach(mapValue -> {
                            DataBeanPropertyBuilder mfield = mappedField(mapKey,
                                    ((String) mapValue), bean, hasCollection);
                            if (mfield != null) mappedProperties.add(mfield);
                            else System.out.println("WARNING: no mapping fileds for " +
                                    bean.interfaceType + "." + mapKey + " is found");
                        });
            }
        }
        manyToOne = hasCollection.get();
    }

    Mapping(DataBeanPropertyBuilder property, DataBeanBuilder mappedBean) {
        builder = property.dataBean.rootBuilder;
        properties.add(property);
        manyToOne = false;
        mappedBean.properties.values().stream()
                .filter(p -> p.isId).forEach(mappedProperties::add);
    }

    private Mapping(DataBeanPropertyBuilder property, DataBeanPropertyBuilder mappedBeanProperty, boolean manyToOne) {
        builder = property.dataBean.rootBuilder;
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

    private DataBeanPropertyBuilder mappedField(String aParam, String enumName, DataBeanBuilder bean, AtomicBoolean hasCollection) {
        final String shortEnumName = enumName.substring(enumName.lastIndexOf('.') + 1);
        DataBeanPropertyBuilder mappedBy = bean.properties.values().stream()
                .filter(p -> p.name.equals(aParam)).findAny().orElse(null);
        if (mappedBy != null) {
            TypeMirror beanType;
            if (mappedBy.notCollectionOrMap()) {
                beanType = mappedBy.type;
            } else if (mappedBy.isCollection) {
                hasCollection.set(true);
                beanType = ((DeclaredType) mappedBy.type).getTypeArguments().get(0);
            } else { //map
                hasCollection.set(true);
                beanType = ((DeclaredType) mappedBy.type).getTypeArguments().get(1);
            }
            DataBeanBuilder mappedBean = builder.beansByInterface.get(
                    builder.environment.getTypeUtils().erasure(beanType).toString()
            );
            return mappedBean == null ? null : mappedBean.properties.values().stream()
                    .filter(p -> p.enumName.equals(shortEnumName)).findAny().orElse(null);
        }
        return null;
    }
}
