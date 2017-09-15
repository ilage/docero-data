package org.docero.data.processor;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

class DDataBuilder {
    private final ProcessingEnvironment environment;

    final HashMap<String, DataBeanBuilder> beansByTable = new HashMap<>();
    final HashMap<String, DataBeanBuilder> beansByInterface = new HashMap<>();
    final ArrayList<DataRepositoryBuilder> repositories = new ArrayList<>();
    final HashMap<String, DataRepositoryBuilder> repositoriesByBean = new HashMap<>();
    final HashSet<String> packages = new HashSet<>();

    DDataBuilder(ProcessingEnvironment environment) {
        this.environment = environment;
    }

    void checkInterface(Element beanElement, TypeMirror collectionType) {
        String typeName = beanElement.asType().toString();
        packages.add(typeName.substring(0, typeName.lastIndexOf('.')));
        DataBeanBuilder value = new DataBeanBuilder(beanElement, environment, collectionType);
        beansByInterface.put(value.interfaceType.toString(), value);
        String key = value.schema + "/" + value.table;
        beansByTable.put(key, value);
    }

    void checkRepository(Element repositoryElement) {
        String typeName = repositoryElement.asType().toString();
        packages.add(typeName.substring(0, typeName.lastIndexOf('.')));
        DataRepositoryBuilder builder =
                new DataRepositoryBuilder(
                        (TypeElement) repositoryElement,
                        environment, beansByInterface);
        repositoriesByBean.put(builder.forInterfaceName(), builder);
        repositories.add(builder);
    }

    void generateClasses() throws IOException {
        for (DataBeanBuilder bean : beansByTable.values()) {
            bean.build(environment, beansByInterface);
        }
        for (DataRepositoryBuilder repositoryBuilder : repositories) {
            repositoryBuilder.build(environment);
        }
        for (DataBeanBuilder bean : beansByTable.values()) {
            if (!repositoriesByBean.containsKey(bean.interfaceType.toString())) {
                DataRepositoryBuilder r = DataRepositoryBuilder.build(bean, environment, beansByInterface);
                repositoriesByBean.put(bean.interfaceType.toString(), r);
                repositories.add(r);
            }
        }
    }
}
