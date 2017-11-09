package org.docero.data;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DDataDictionariesService {
    private final ConcurrentHashMap<Class, DDataRepository> dictionaries =
            new ConcurrentHashMap<>();

    DDataDictionariesService() {
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
    <T extends Serializable, C extends Serializable> void register(Class<? extends T> type, DDataRepository<T, C> repository) {
        dictionaries.put(type, repository);
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
        DDataRepository<T, ? extends Serializable> d = dictionaries.get(bean.getClass());
        try {
            Method mput = d.getClass().getDeclaredMethod("put_", bean.getClass());
            if (mput != null) {
                mput.setAccessible(true);
                mput.invoke(d, bean);
            }
            Method m = bean.getClass().getDeclaredMethod("setDictionariesService_", DDataDictionariesService.class);
            if (m != null) {
                m.setAccessible(true);
                m.invoke(bean, this);
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignore) {
        }
        return bean;
    }

    /**
     * Store collection of beans in dictionary repository cache
     *
     * @param beans - collection of dictionary beans
     */
    public <T extends Serializable> void put(Collection<T> beans) {
        if (beans != null && beans.size() > 0) {
            @SuppressWarnings("unchecked")
            Class<T> type = (Class<T>) beans.iterator().next().getClass();
            @SuppressWarnings("unchecked")
            DDataRepository<T, ? extends Serializable> d = dictionaries.get(type);
            if (d != null) try {
                Method mput = d.getClass().getDeclaredMethod("put_", type);
                if (mput != null) {
                    mput.setAccessible(true);
                    for (T bean : beans) mput.invoke(d, bean);
                }

                Method mget = type.getDeclaredMethod("getDDataBeanKey_");
                List<? extends Serializable> keys = beans.stream()
                        .map(bean -> {
                            try {
                                return (Serializable) mget.invoke(bean);
                            } catch (IllegalAccessException | InvocationTargetException ignore) {
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                mput = d.getClass().getDeclaredMethod("putList_", keys.getClass());
                if (mput != null) {
                    mput.setAccessible(true);
                    mput.invoke(d, keys);
                }
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignore) {
            }
        }
    }
}
