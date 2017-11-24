package org.docero.data.processor;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.util.HashMap;

class MapBuilder extends MapElement {
    private final String id;
    private final String methodName;
    private final DataRepositoryBuilder repository;

    MapBuilder(String methodName, DataRepositoryBuilder repository) {
        id = "";
        this.methodName = methodName;
        this.repository = repository;
    }

    @Override
    String id() {
        return id;
    }

    @Override
    void write(Node mapperRoot) {
        Document doc = mapperRoot.getOwnerDocument();
        org.w3c.dom.Element map = (org.w3c.dom.Element)
                mapperRoot.appendChild(doc.createElement("resultMap"));
        map.setAttribute("id", methodName + "_ResultMap");
        map.setAttribute("type", repository.beanImplementation);
        super.write(map);
        if (discriminatorElements.size() > 0) {
            Element elt = (Element) map.appendChild(doc.createElement("discriminator"));
            elt.setAttribute("javaType", discriminator.property.type.toString());
            elt.setAttribute("column", discriminator.property.columnName);
            for (DiscriminatorElement discriminatorElement : discriminatorElements.values()) {
                discriminatorElement.write(elt);
                discriminatorElement.writeMap(mapperRoot);
            }
        }
    }

    @Override
    String fromInterface() {
        assert repository.forInterfaceName != null;
        return repository.forInterfaceName.toString();
    }

    private final HashMap<String, MapBuilder.DiscriminatorElement> discriminatorElements = new HashMap<>();
    private DataRepositoryDiscriminator discriminator;

    MapElement getDiscriminatorElement(DataRepositoryDiscriminator.Item discriminator) {
        MapBuilder.DiscriminatorElement elem = discriminatorElements.get(discriminator.beanInterface);
        if (elem == null) {
            elem = new MapBuilder.DiscriminatorElement(discriminator, this);
            if (discriminatorElements.size() == 0)
                this.discriminator = discriminator.parent;
            discriminatorElements.put(discriminator.beanInterface, elem);
        }
        return elem;
    }

    MapElement getDiscriminatorElement(String beanInterface) {
        //if(discriminatorElements.size()==0) return null;
        return discriminatorElements.get(beanInterface);
    }

    @Override
    MapBuilder mapBuilder() {
        return this;
    }

    @Override
    boolean isCollection() {
        return false;
    }

    @Override
    boolean notCollection() {
        return true;
    }

    static class MappedElement extends MapElement {
        final String id;
        final DDataMapBuilder.MappedTable mappedTable;
        final boolean isCollection;
        final MapBuilder parent;

        MappedElement(DDataMapBuilder.MappedTable table, MapBuilder parent) {
            id = Integer.toString(table.tableIndex);
            this.mappedTable = table;
            this.isCollection = mappedTable.property.isCollectionOrMap();
            this.parent = parent;
        }

        @Override
        String id() {
            return id;
        }

        @Override
        boolean isCollection() {
            return isCollection;
        }

        @Override
        boolean notCollection() {
            return !isCollection;
        }

        @Override
        MapElement getDiscriminatorElement(DataRepositoryDiscriminator.Item discriminator) {
            return parent.getDiscriminatorElement(discriminator);
        }

        @Override
        MapBuilder mapBuilder() {
            return parent;
        }

        @Override
        void write(Node map) {
            org.w3c.dom.Element managed = (org.w3c.dom.Element) map.appendChild(map.getOwnerDocument().createElement(
                    isCollection ? "collection" : "association"));
            managed.setAttribute("property", mappedTable.property.name);
            managed.setAttribute("javaType", mappedTable.property.isCollection ? "ArrayList" : (
                    mappedTable.property.isMap ? "HashMap" : mappedTable.mappedBean.getImplementationName())
            );
            super.write(managed);
        }

        @Override
        String fromInterface() {
            return mappedTable.property.dataBean.interfaceType.toString();
        }
    }

    static class DiscriminatorElement extends MapElement {
        final String id;
        final DataRepositoryDiscriminator.Item discriminator;
        final MapBuilder parent;

        DiscriminatorElement(DataRepositoryDiscriminator.Item discriminator, MapBuilder map) {
            this.discriminator = discriminator;
            parent = map;
            id = discriminator.beanInterface;
        }

        String resultMapId() {
            return discriminator.beanInterface
                    .substring(discriminator.beanInterface.lastIndexOf('.') + 1) +
                    "_extention";
        }

        @Override
        String id() {
            return id;
        }

        @Override
        void write(Node map) {
            org.w3c.dom.Element dcase = (org.w3c.dom.Element)
                    map.appendChild(map.getOwnerDocument().createElement("case"));
            dcase.setAttribute("value", discriminator.value);
            dcase.setAttribute("resultMap", resultMapId());
        }

        @Override
        String fromInterface() {
            return discriminator.beanInterface;
        }

        void writeMap(Node root) {
            Element map = root.getOwnerDocument().createElement("resultMap");
            setAttribute("id", resultMapId());
            setAttribute("type", discriminator.beanInterface + "Impl");
            setAttribute("extends", parent.methodName + "_ResultMap");
            root.appendChild(map);

            attributes.forEach(map::setAttribute);

            for (ResultElement resultElement : resultElements)
                if (parent.resultElements.stream().noneMatch(re -> re.property.columnName.equals(resultElement.property.columnName)))
                    resultElement.write(map);
            for (MapElement association : mappedElements)
                if (parent.mappedElements.stream().noneMatch(re -> re.id().equals(association.id())))
                    if (association.notCollection()) association.write(map);
            for (MapElement collection : mappedElements)
                if (parent.mappedElements.stream().noneMatch(re -> re.id().equals(collection.id())))
                    if (collection.isCollection()) collection.write(map);
        }

        @Override
        MapElement getDiscriminatorElement(DataRepositoryDiscriminator.Item discriminator) {
            return this;
        }

        @Override
        MapBuilder mapBuilder() {
            return parent;
        }

        @Override
        boolean isCollection() {
            return false;
        }

        @Override
        boolean notCollection() {
            return true;
        }
    }
}
