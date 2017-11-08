package org.docero.data.utils;

import org.apache.ibatis.reflection.factory.ObjectFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class DDataObjectFactory implements ObjectFactory {
    private final DDataDictionariesService dictionariesService;

    public DDataObjectFactory(DDataDictionariesService dictionariesService) {
        this.dictionariesService = dictionariesService;
    }

    @Override
    public void setProperties(Properties properties) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T create(Class<T> type) {
        if (List.class.isAssignableFrom(type)) return (T) new ArrayList();
        if (Map.class.isAssignableFrom(type)) return (T) new HashMap();
        if (Set.class.isAssignableFrom(type)) return (T) new HashSet();

        T bean = null;
        if (Serializable.class.isAssignableFrom(type))
            bean = (T) dictionariesService.create((Class<? extends Serializable>) type);
        if (bean == null)
            try {
                bean = type.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                bean = null;
            }
        if (bean != null)
            try {
                Method m = bean.getClass().getDeclaredMethod("setDictionariesService_", DDataDictionariesService.class);
                m.setAccessible(true);
                m.invoke(bean, dictionariesService);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignore) {
            }
        return bean;
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
