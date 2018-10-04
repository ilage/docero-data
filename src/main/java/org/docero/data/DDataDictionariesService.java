package org.docero.data;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.remote.DDataRemoteDictionary;
import org.docero.data.remote.DDataRemoteRepository;
import org.docero.data.utils.DDataDictionary;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DDataDictionariesService {
    private final ConcurrentHashMap<Class, Object> repositories =
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
    @SuppressWarnings("unchecked")
    public <T extends Serializable> T create(Class<T> type) {
        Object d = repositories.get(type);
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
            repositories.put(type, repository);
    }

    /**
     * Add remote repository to list
     *
     * @param type       - class of bean implemetation
     * @param repository - remote repository
     */
    public <T extends Serializable, C extends Serializable> void register(Class<? extends T> type, DDataRemoteRepository<T, C> repository) {
        repositories.put(type, repository);
    }

    /**
     * Get bean by dictionary repository
     *
     * @param type - class of bean
     * @param key  - bean id
     * @return bean of specified type with specified id
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable, C extends Serializable> T get(Class<T> type, C key) {
        Object d = repositories.get(type);
        if (d == null) return null;
        return d instanceof DDataRepository ?
                ((DDataRepository<T, C>) d).get(key) :
                (d instanceof DDataRemoteRepository ?
                        get((DDataRemoteRepository<T, C>) d, null, new Object[]{key}) :
                        null);
    }

    /**
     * Get bean by dictionary repository
     *
     * @param type class of bean
     * @param func method name
     * @param args method parameters
     * @return bean of specified type with specified id
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable, C extends Serializable> T get(Class<T> type, String func, Object[] args) {
        if (args == null) return null;
        Object d = repositories.get(type);
        if (d != null && d instanceof DDataRemoteRepository)
            return get((DDataRemoteRepository<T, C>) d, func, args);
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T extends Serializable, C extends Serializable> T get(
            DDataRemoteRepository<T, C> rr, String func, Object[] args
    ) {
        Class<?>[] pc = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) pc[i] = args[i].getClass();
        try {
            Method m = rr.getClass().getMethod(func == null ? "get" : func, pc);
            return (T) m.invoke(rr, args);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignore) {
            return null;
        }
    }

    /**
     * Store bean in dictionary repository cache
     *
     * @param bean - dictionary bean (object)
     * @return bean
     */
    @SuppressWarnings("unchecked")
    public <T extends Serializable> T put(T bean) {
        if (bean == null) return null;
        Object d = repositories.get(bean.getClass());
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
        Object d = repositories.get(type);
        if (d instanceof DDataDictionary && ((DDataDictionary) d).version_() != 0)
            ((DDataDictionary) d).version_((int) System.currentTimeMillis());
    }

    public <T extends Serializable> void clearVersion(Class<T> type) {
        Object d = repositories.get(type);
        if (d instanceof DDataDictionary)
            ((DDataDictionary) d).version_(0);
    }

    @SuppressWarnings({"unchecked"})
    public <T extends Serializable, C extends Serializable> List<T> list(
            Class<T> type, SqlSession session, String selectId
    ) {
        Object o = repositories.get(type);
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
