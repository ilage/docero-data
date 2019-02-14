package org.docero.data;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.docero.data.remote.DDataRemoteRepository;
import org.docero.data.utils.DDataModule;
import org.docero.data.utils.DDataObjectFactory;
import org.docero.data.view.DDataViewBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DData {
    private static final Logger LOG = LoggerFactory.getLogger(DData.class);
    private static final ConcurrentHashMap<Class, DDataModule> modules = new ConcurrentHashMap<>();
    private static final DDataDictionariesService dictionariesService = new DDataDictionariesService();

    private final SqlSessionFactory sessionFactory;
    private final Map<Class<?>, Object> buildedRepositories = new ConcurrentHashMap<>();

    public DData(SqlSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        modules.values().forEach(m -> m.register(this, dictionariesService));
    }

    public DDataViewBuilder getViewBuilder() {
        return new DDataViewBuilder(sessionFactory, dictionariesService);
    }


    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    public <T extends Serializable, C extends Serializable> DDataRepository<T, C>
    getBeanRepository(Class<T> beanClass) {
        Object r = buildedRepositories.get(beanClass);
        if (r != null) return (DDataRepository<T, C>) r;

        DDataModule module = modules.values().stream()
                .filter(m -> m.getImplementations().containsKey(beanClass))
                .findAny().orElse(null);
        if (module == null)
            throw new RuntimeException("unknown d.data repository for " + beanClass.getName());

        buildedRepositories.put(beanClass, r = checkSingletonRepository(
                    module.getBeanRepository(beanClass, sessionFactory)));
        return (DDataRepository<T, C>) r;
    }

    @SuppressWarnings("unchecked")
    public <R> R getRepository(Class<R> clazz) {
        R r = (R) buildedRepositories.get(clazz);
        if (r != null) return r;

        for (DDataModule module : modules.values()) {
            r = module.getRepositoryByInterface(clazz, sessionFactory);
            if (r != null) {
                buildedRepositories.put(clazz, r = checkSingletonRepository(r));
                return r;
            }
        }
        throw new RuntimeException("unknown d.data repository " + clazz.getName());
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> T checkSingletonRepository(T repository) {
        if (repository == null) return null;

        T exists = (T) buildedRepositories.get(repository.getClass());
        if (exists == null) {
            buildedRepositories.put(repository.getClass(), repository);
            return repository;
        }
        return exists;
    }

    /*
        Static methods
     */

    @SafeVarargs
    public static String[] resources(Class<? extends DDataModule>... modules) {
        for (Class<? extends DDataModule> module : modules)
            if (!DData.modules.containsKey(module))
                try {
                    DData.modules.put(module, module.newInstance());
                } catch (InstantiationException | IllegalAccessException e) {
                    LOG.warn("can't instantiate " + module.getName(), e);
                }
        return DData.modules.values().stream()
                .flatMap(m -> Arrays.stream(m.resources()))
                .toArray(String[]::new);
    }

    public static java.util.Map<Class<?>, Class<?>> getImplementations() {
        return modules.values().stream()
                .flatMap(m -> m.getImplementations().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static String[] getCacheNames() {
        return modules.values().stream()
                .flatMap(m -> Arrays.stream(m.getCacheNames()))
                .toArray(String[]::new);
    }

    public static org.apache.ibatis.reflection.factory.ObjectFactory getObjectFactory() {
        return new DDataObjectFactory();
    }

    public static Map<Class<?>, com.fasterxml.jackson.databind.JsonDeserializer<?>> getDeserializers() {
        return modules.values().stream()
                .flatMap(m -> m.getDeserializers().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /*
        Dictionaries access
     */

    public static <P extends Serializable> P cache(Class<P> type, Serializable key) {
        return dictionariesService.get(type, key);
    }

    public static <P extends Serializable> P remote(Class<P> type, String func, Serializable[] key) {
        return dictionariesService.get(type, func, key);
    }

    public static <T extends Serializable, C extends Serializable, R extends DDataRemoteRepository<T, C>>
    R registerRemote(R remote, Class<? extends T> remoteBeanClass
    ) {
        return dictionariesService.register(remoteBeanClass, remote);
    }

    static <T extends Serializable> List<T> list(Class<T> type, SqlSession session, String selectId) {
        return dictionariesService.list(type, session, selectId);
    }

    static <T extends Serializable> void updateVersion(Class<T> type) {
        dictionariesService.updateVersion(type);
    }

    static <T extends Serializable> void clearVersion(Class<T> type) {
        dictionariesService.clearVersion(type);
    }

    static <T extends Serializable> void cache(T bean) {
        dictionariesService.put(bean);
    }
}