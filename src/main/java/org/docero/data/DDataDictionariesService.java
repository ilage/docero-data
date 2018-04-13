package org.docero.data;

import org.docero.data.utils.DDataDictionary;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DDataDictionariesService {
    private final ConcurrentHashMap<Class, DDataDictionary> dictionaries =
            new ConcurrentHashMap<>();

    public DDataDictionariesService() {
    }

    /**
     * Create bean by dictionary with mapped type
     *
     * @param type - class of bean
     * @return bean of specified type
     */
    public <T extends Serializable> T create(Class<T> type) {
        @SuppressWarnings("unchecked")
        DDataRepository<T, ? extends Serializable> d = dictionaries.get(type);
        if (d == null) return null;
        return d.create();
    }

    /**
     * Add dictionary repository to list
     *
     * @param type       - class of bean interface or implementation
     * @param repository - dictionary repository
     */
    public <T extends Serializable, C extends Serializable> void register(Class<? extends T> type, DDataRepository<T, C> repository) {
        if (repository instanceof DDataDictionary) dictionaries.put(type, (DDataDictionary) repository);
    }

    /**
     * Get bean by dictionary repository
     *
     * @param type - class of bean
     * @param key  - bean id
     * @return bean of specified type with specified id
     */
    public <T extends Serializable, C extends Serializable> T get(Class<T> type, C key) {
        @SuppressWarnings("unchecked")
        DDataRepository<T, C> d = dictionaries.get(type);
        if (d == null) return null;
        return d.get(key);
    }

    /**
     * Store bean in dictionary repository cache
     *
     * @param bean - dictionary bean (object)
     * @return bean
     */
    public <T extends Serializable> T put(T bean) {
        if (bean == null) return null;
        @SuppressWarnings("unchecked")
        DDataDictionary<T, ? extends Serializable> d = dictionaries.get(bean.getClass());
        if (d != null) d.put_(bean);
        return bean;
    }

    /**
     * Store collection of beans in dictionary repository cache
     *
     * @param beans - collection of dictionary beans
     */
    public <T extends Serializable, C extends Serializable> void put(Collection<T> beans) {
        if (beans != null && beans.size() > 0) {
            @SuppressWarnings("unchecked")
            Class<T> type = (Class<T>) beans.iterator().next().getClass();
            @SuppressWarnings("unchecked")
            DDataDictionary<T, C> d = dictionaries.get(type);
            if (d != null) {
                for (T bean : beans) d.put_(bean);
                try {
                    @SuppressWarnings("JavaReflectionMemberAccess")
                    Method mget = type.getDeclaredMethod("getDDataBeanKey_");
                    List<C> keys = beans.stream()
                            .map(bean -> {
                                try {
                                    //noinspection unchecked
                                    return (C) mget.invoke(bean);
                                } catch (IllegalAccessException | InvocationTargetException ignore) {
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                    d.putList_(keys);
                } catch (NoSuchMethodException ignore) {
                }
            }
        }
    }
}
