package org.docero.data;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.utils.DDataDictionary;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DDataDictionariesService {
    private final ConcurrentHashMap<Class, DDataDictionary> dictionaries =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class, Integer> versions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class, List> lists = new ConcurrentHashMap<>();

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


    public <T extends Serializable> void updateVersion(Class<T> type) {
        DDataDictionary d = dictionaries.get(type);
        if (d.version_() != 0)
            d.version_((int) System.currentTimeMillis());
    }

    public <T extends Serializable> void clearVersion(Class<T> type) {
        DDataDictionary d = dictionaries.get(type);
        d.version_(0);
    }

    @SuppressWarnings({"unchecked", "JavaReflectionMemberAccess"})
    public <T extends Serializable, C extends Serializable> List<T> list(
            Class<T> type, SqlSession session, String selectId
    ) {
        DDataDictionary<T, C> d = dictionaries.get(type);
        Integer localListVersion = versions.get(type);
        Integer cv = d.version_();
        if (cv.equals(localListVersion)) {
            List<T> cached = new ArrayList<>();
            List<C> keys = lists.get(type);
            if (keys != null) keys.forEach(id -> cached.add(d.get(id)));
            return cached;
        } else {
            List<T> selected = session.selectList(selectId);

            boolean initialLoad = d.version_() == 0;
            if (!selected.isEmpty())
                try {
                    Method mget = selected.get(0).getClass().getMethod("getDDataBeanKey_");
                    ArrayList<Serializable> keys = new ArrayList();
                    if (initialLoad)
                        for (T bean : selected) keys.add((Serializable) mget.invoke(d.put_(bean)));
                    else
                        for (T bean : selected) keys.add((Serializable) mget.invoke(bean));
                    lists.put(type, keys);
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignore) {
                }
            if (initialLoad) cv = d.version_(1);
            versions.put(type, cv);
            return selected;
        }
    }
}
