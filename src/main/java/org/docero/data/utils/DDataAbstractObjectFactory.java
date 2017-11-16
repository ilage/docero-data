package org.docero.data.utils;

import org.apache.ibatis.reflection.factory.ObjectFactory;
import java.util.*;

public abstract class DDataAbstractObjectFactory implements ObjectFactory {
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
