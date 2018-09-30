package org.docero.data;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.utils.DDataDictionary;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DDataDictionariesService {
    private final ConcurrentHashMap<Class, Object> dictionaries =
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
        Object d = dictionaries.get(type);
        if (d == null) return null;
        try {
            return d instanceof DDataRepository ?
                    ((DDataRepository<T, ?>) d).create() :
                    (d instanceof DDataRemoteRepository ?
                            type.newInstance() :
                            null);
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    /**
     * Add dictionary repository to list
     *
     * @param type       - class of bean interface or implementation
     * @param repository - dictionary repository
     */
    public <T extends Serializable, C extends Serializable> void register(Class<? extends T> type, DDataRepository<T, C> repository) {
        if (repository instanceof DDataDictionary)
            dictionaries.put(type, repository);
    }

    /**
     * Add remote repository to list
     *
     * @param type       - class of bean implemetation
     * @param repository - remote repository
     */
    public <T extends Serializable, C extends Serializable> void register(Class<? extends T> type, DDataRemoteRepository<T, C> repository) {
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
        Object d = dictionaries.get(type);
        if (d == null) return null;
        return d instanceof DDataRepository ?
                ((DDataRepository<T, C>) d).get(key) :
                (d instanceof DDataRemoteRepository ?
                        ((DDataRemoteRepository<T, C>) d).get(key) :
                        null);
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
        Object d = dictionaries.get(bean.getClass());
        if (d != null && d instanceof DDataDictionary)
            ((DDataDictionary<T, ? extends Serializable>) d).put_(bean);
        return bean;
    }

    /*
     * Version used for optimistic locks of 'lists' property entries, we read current list (from 'lists')
     * if version in cache equals version from 'versions' property.
     * Property 'lists' contains lists of bean primary keys mapped by bean interface.
     * Not a very important than someone read zero from version or not but in most cases
     * it will do elements loading faster.
     */
    public <T extends Serializable> void updateVersion(Class<T> type) {
        Object d = dictionaries.get(type);
        if (d instanceof DDataDictionary && ((DDataDictionary) d).version_() != 0)
            ((DDataDictionary) d).version_((int) System.currentTimeMillis());
    }

    public <T extends Serializable> void clearVersion(Class<T> type) {
        Object d = dictionaries.get(type);
        if (d instanceof DDataDictionary)
            ((DDataDictionary) d).version_(0);
    }

    @SuppressWarnings({"unchecked"})
    public <T extends Serializable, C extends Serializable> List<T> list(
            Class<T> type, SqlSession session, String selectId
    ) {
        Object o = dictionaries.get(type);
        Integer localListVersion = versions.get(type);

        if (o instanceof DDataRemoteDictionary) {
            DDataRemoteDictionary<T, C> d = (DDataRemoteDictionary<T, C>) o;
            Integer cv = d.version_();
            if (cv.equals(localListVersion)) {
                return lists.get(type);
            } else {
                List<T> selected = session.selectList(selectId);
                lists.put(type, selected);
                versions.put(type, cv);
                return selected;
            }
        } else if (o instanceof DDataDictionary) {
            DDataDictionary<T, C> d = (DDataDictionary<T, C>) o;
            Integer cv = d.version_();
            if (cv.equals(localListVersion)) {
                return lists.get(type);
            } else {
                List<T> selected = session.selectList(selectId);
                lists.put(type, selected);
                boolean initialLoad = d.version_() == 0;
                if (initialLoad) {
                    selected.forEach(d::put_);
                    cv = d.version_(1);
                }
                versions.put(type, cv);
                return selected;
            }
        }
        return Collections.emptyList();
    }
}
