package org.docero.data.processor;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.lang.model.type.TypeMirror;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

abstract class MapElement {
    final List<IdElement> idElements = new ArrayList<>();
    final List<ResultElement> resultElements = new ArrayList<>();
    final List<MapElement> mappedElements = new ArrayList<>();
    final HashMap<String, String> attributes = new HashMap<>();

    TypeMirror useVersionalProperty;

    abstract String id();

    abstract boolean isCollection();

    abstract boolean notCollection();

    abstract MapElement getDiscriminatorElement(DataRepositoryDiscriminator.Item discriminator);

    abstract MapBuilder mapBuilder();


    void write(Node map) {
        if (map instanceof Element) attributes.forEach(((Element) map)::setAttribute);

        for (IdElement idElement : idElements)
            idElement.write(map);
        for (ResultElement resultElement : resultElements)
            resultElement.write(map);
        if (useVersionalProperty!=null && this == mapBuilder()) {
            org.w3c.dom.Element id = (org.w3c.dom.Element)
                    map.appendChild(map.getOwnerDocument().createElement("result"));
            id.setAttribute("property", "dDataBeanActualAt_");
            id.setAttribute("column", "t0_dDataBeanActualAt_");
            id.setAttribute("javaType", useVersionalProperty.toString());
        }
        for (MapElement association : mappedElements)
            if (association.notCollection()) association.write(map);
        for (MapElement collection : mappedElements)
            if (collection.isCollection()) collection.write(map);
    }

    void addId(String prefix, DataBeanPropertyBuilder property) {
        idElements.add(new IdElement(property, prefix));
    }

    void addResult(String prefix, DataBeanPropertyBuilder p) {
        resultElements.add(new ResultElement(p, prefix));
    }

    void setAttribute(String id, String value) {
        attributes.put(id, value);
    }

    final MapElement createTableElement(DDataMapBuilder.MappedTable table) {
        return new MapBuilder.MappedElement(table, mapBuilder());
    }

    final MapElement addTable(MapElement map) {
        mappedElements.add(map);
        return map;
    }

    abstract String fromInterface();

    void setVersionalProperty(TypeMirror versionalType) {
        useVersionalProperty = versionalType;
    }

    static class IdElement {
        final DataBeanPropertyBuilder property;
        final String prefix;

        IdElement(DataBeanPropertyBuilder property, String prefix) {
            this.property = property;
            this.prefix = prefix;
        }

        void write(Node map) {
            org.w3c.dom.Element id = (org.w3c.dom.Element)
                    map.appendChild(map.getOwnerDocument().createElement("id"));
            id.setAttribute("property", property.name);
            id.setAttribute("column", prefix + property.columnName);
        }
    }

    static class ResultElement {
        final DataBeanPropertyBuilder property;
        final String prefix;

        ResultElement(DataBeanPropertyBuilder property, String prefix) {
            this.property = property;
            this.prefix = prefix;
        }

        void write(Node map) {
            org.w3c.dom.Element id = (org.w3c.dom.Element)
                    map.appendChild(map.getOwnerDocument().createElement("result"));
            id.setAttribute("property", property.name);
            id.setAttribute("column", prefix + property.columnName);
        }
    }
}
