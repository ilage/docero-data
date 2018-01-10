package org.docero.data.processor;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.TypeElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DataRepositoryDiscriminator {
    final DataBeanPropertyBuilder property;
    final Item[] beans;

    DataRepositoryDiscriminator(DataBeanBuilder bean, TypeElement repositoryElement, AnnotationMirror da, ProcessingEnvironment environment) {
        AnnotationValue value = environment.getElementUtils()
                .getElementValuesWithDefaults(da).entrySet().stream()
                .filter(e -> "value".equals(e.getKey().getSimpleName().toString()))
                .findAny().map(Map.Entry::getValue).orElse(null);
        assert value != null;
        String fullEnumName = value.getValue().toString();
        int i = fullEnumName.lastIndexOf('.');
        String enumName = i > 0 ? fullEnumName.substring(i + 1) : fullEnumName;
        property = bean.properties.values().stream()
                .filter(p -> p.enumName.equals(enumName))
                .findAny().orElse(null);

        AnnotationMirror ra = repositoryElement.getAnnotationMirrors().stream()
                .filter(m -> m.getAnnotationType().toString().endsWith("DDataRep"))
                .findAny().orElse(null);
        AnnotationValue beansValue = environment.getElementUtils()
                .getElementValuesWithDefaults(ra).entrySet().stream()
                .filter(e -> "discriminator".equals(e.getKey().getSimpleName().toString()))
                .findAny().map(Map.Entry::getValue).orElse(null);
        assert beansValue != null;
        List<Item> items = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<AnnotationMirror> discriminators = (List<AnnotationMirror>) beansValue.getValue();
        for (AnnotationMirror discriminator : discriminators) {
            String dv = discriminator.getElementValues().entrySet().stream()
                    .filter(e -> "value".equals(e.getKey().getSimpleName().toString()))
                    .findAny().map(e -> e.getValue().getValue().toString())
                    .orElse(null);
            String db = discriminator.getElementValues().entrySet().stream()
                    .filter(e -> "bean".equals(e.getKey().getSimpleName().toString()))
                    .findAny().map(e -> e.getValue().getValue().toString())
                    .orElse(null);
            items.add(new Item(dv, db, this));
        }
        this.beans = items.toArray(new Item[items.size()]);
    }

    class Item {
        final DataRepositoryDiscriminator parent;
        final String value;
        final String beanInterface;

        Item(String value, String beanInterface, DataRepositoryDiscriminator parent) {
            this.value = value;
            this.beanInterface = beanInterface;
            this.parent = parent;
        }

        String beanInterfaceShort() {
            return beanInterface.substring(beanInterface.lastIndexOf('.') + 1);
        }
    }
}
