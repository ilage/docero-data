package org.docero.data.processor;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

class Mapping {
    final List<DataBeanPropertyBuilder> properties = new ArrayList<>();
    final List<DataBeanPropertyBuilder> mappedProperties = new ArrayList<>();
    final String func;
    final boolean manyToOne;
    final boolean markTransient;
    final boolean alwaysLazy;

    private final DDataBuilder builder;

    Mapping(AnnotationMirror annotationMirror, DataBeanBuilder bean) {
        builder = bean.rootBuilder;
        Map<? extends ExecutableElement, ? extends AnnotationValue> map =
                builder.environment.getElementUtils().getElementValuesWithDefaults(annotationMirror);
        boolean markTransient = false;
        boolean alwaysLazy = false;
        String func = null;
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
            } else if ("markTransient".equals(mapKey)) {
                markTransient = Boolean.parseBoolean(map.get(executableElement).getValue().toString());
            } else if ("alwaysLazy".equals(mapKey)) {
                alwaysLazy = Boolean.parseBoolean(map.get(executableElement).getValue().toString());
            } else if ("func".equals(mapKey)) {
                func = map.get(executableElement).getValue().toString();
            } else {
                //noinspection unchecked
                ((List) map.get(executableElement).getValue()).stream()
                        .map(Object::toString)
                        .forEach(mapValue -> {
                            DataBeanPropertyBuilder mfield = mappedField(mapKey,
                                    ((String) mapValue), bean, hasCollection);
                            if (mfield != null) mappedProperties.add(mfield);
                            else {
                                builder.environment.getMessager().printMessage(Diagnostic.Kind.WARNING,
                                        "no mapping fields for " + bean.interfaceType + "." + mapKey + " was found");
                            }
                        });
            }
        }
        if (mappedProperties.isEmpty()) {
            bean.properties.values().stream()
                    .filter(p -> p.isId).forEach(mappedProperties::add);
            if (properties.size() != mappedProperties.size())
                mappedProperties.clear();
        }
        this.markTransient = markTransient;
        this.alwaysLazy = alwaysLazy;
        this.func = func != null && func.length() == 0 ? null : func;
        manyToOne = hasCollection.get();
    }

    Mapping(DataBeanPropertyBuilder property, DataBeanBuilder mappedBean) {
        builder = property.dataBean.rootBuilder;
        properties.add(property);
        manyToOne = false;
        mappedBean.properties.values().stream()
                .filter(p -> p.isId).forEach(mappedProperties::add);
        markTransient = false;
        alwaysLazy = false;
        func = null;
    }

    private Mapping(DataBeanPropertyBuilder property, DataBeanPropertyBuilder mappedBeanProperty, boolean manyToOne) {
        builder = property.dataBean.rootBuilder;
        properties.add(property);
        mappedProperties.add(mappedBeanProperty);
        this.manyToOne = manyToOne;
        markTransient = false;
        alwaysLazy = false;
        func = null;
    }

    class SingleFieldMapping {
        final DataBeanPropertyBuilder property;
        final DataBeanPropertyBuilder mappedProperty;
        final boolean manyToOne;

        SingleFieldMapping(DataBeanPropertyBuilder property, DataBeanPropertyBuilder mappedProperties, boolean manyToOne) {
            this.property = property;
            this.mappedProperty = mappedProperties;
            this.manyToOne = manyToOne;
        }
    }

    Stream<SingleFieldMapping> stream() {
        List<SingleFieldMapping> l = new ArrayList<>();
        if (properties.size() > 0 && mappedProperties.size() > 0)
            for (int i = 0; i < Math.max(properties.size(), mappedProperties.size()); i++) {
                l.add(new SingleFieldMapping(
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
