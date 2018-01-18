package org.docero.data.processor;

import java.util.List;

class DataRepositoryDiscriminator {
    final DataBeanPropertyBuilder property;
    final Item[] beans;

    DataRepositoryDiscriminator(List<DataBeanBuilder> beans) {
        this.beans = beans.stream()
                .map(b->new Item(b.discriminatorValue,b.interfaceType.toString(),this))
                .toArray(Item[]::new);
        property = beans.get(0).discriminatorProperty;
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
