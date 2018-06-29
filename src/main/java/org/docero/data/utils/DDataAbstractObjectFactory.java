package org.docero.data.utils;

import org.apache.ibatis.reflection.factory.ObjectFactory;

import java.io.Serializable;
import java.util.*;

public abstract class DDataAbstractObjectFactory implements ObjectFactory, Serializable {
    public abstract <T> Class<? extends T> getImplementation(Class<T> type);

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
