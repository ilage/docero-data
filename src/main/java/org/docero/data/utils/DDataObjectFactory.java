package org.docero.data.utils;

import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.docero.data.DData;

import java.io.Serializable;
import java.util.*;

public class DDataObjectFactory implements ObjectFactory, Serializable {
    @SuppressWarnings("unchecked")
    public <T> Class<? extends T> getImplementation(Class<T> type) {
        Class<? extends T> i = (Class<? extends T>) DData.getImplementations().get(type);
        return DData.getImplementations().containsKey(type) ? i : type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T create(Class<T> type) {
        if (List.class.isAssignableFrom(type)) return (T) new ArrayList();
        if (Map.class.isAssignableFrom(type)) return (T) new HashMap();
        if (Set.class.isAssignableFrom(type)) return (T) new HashSet();
        T bean;
        Class<? extends T> implType = getImplementation(type);
        try {
            bean = implType.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            bean = null;
        }
        return bean;
    }

    @Override
    public void setProperties(Properties properties) {

    }

    @Override
    public <T> T create(Class<T> type, List<Class<?>> constructorArgTypes, List<Object> constructorArgs) {
        return null;
    }

    @Override
    public <T> boolean isCollection(Class<T> type) {
        return type != null && Collection.class.isAssignableFrom(type);
    }
}
