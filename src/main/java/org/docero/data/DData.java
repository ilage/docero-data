package org.docero.data;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.docero.data.remote.DDataRemoteRepository;
import org.docero.data.utils.DDataModule;
import org.docero.data.utils.DDataObjectFactory;
import org.docero.data.utils.UpdateOptions;
import org.docero.data.view.DDataViewBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericApplicationContext;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DData {
    private static final Logger LOG = LoggerFactory.getLogger(DData.class);
    private static final ConcurrentHashMap<Class, DDataModule> modules = new ConcurrentHashMap<>();
    private static final DDataDictionariesService dictionariesService = new DDataDictionariesService();

    private final GenericApplicationContext springApplicationContext;
    private final SqlSessionFactory sessionFactory;
    private final Map<Class<?>, Object> buildedRepositories = new ConcurrentHashMap<>();

    public DData(SqlSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        this.springApplicationContext = null;
        modules.values().forEach(m -> m.register(this, dictionariesService));
    }

    @SuppressWarnings("unchecked")
    public DData(SqlSessionFactory sessionFactory, GenericApplicationContext applicationContext) {
        this.sessionFactory = sessionFactory;
        this.springApplicationContext = applicationContext;
        if (applicationContext != null)
            modules.values().forEach(m -> m.getRepositories().forEach((i, r) -> {
                if (r.isAssignableFrom(i))
                    createSpringBean(i, m.getRepositoryByInterface(i, sessionFactory));
            }));
        modules.values().forEach(m -> m.register(this, dictionariesService));
    }

    private <T, R extends T> void createSpringBean(Class<T> i, R r) {
        springApplicationContext.registerBean(i, () -> r);
        buildedRepositories.put(i, springApplicationContext.getBean(i));
    }

    private void extractorOfInterfaces(Class clazz, Set set) {
        for (Class anInterface : clazz.getInterfaces()) {
            if (clazz.getSuperclass() != null) {
                set.add(clazz.getSuperclass());
                extractorOfInterfaces(clazz.getSuperclass(), set);
            }
            set.add(anInterface);
            extractorOfInterfaces(anInterface, set);
        }
    }

    private Set<Class> extractionAllInterfaces(Class clazz) {
        HashSet interfacesOfObject = new LinkedHashSet();
        interfacesOfObject.add(clazz);
        extractorOfInterfaces(clazz, interfacesOfObject);
        return interfacesOfObject;
    }

    public DDataViewBuilder getViewBuilder() {
        return new DDataViewBuilder(sessionFactory, dictionariesService);
    }

    public <T extends Serializable> T save(T t, UpdateOptions updateOptions) {
        Class beanInterface = null;
        DDataModule module = null;
        for (Class interfaceOfBean : extractionAllInterfaces(t.getClass())) {
            for (DDataModule m : modules.values()) {
                if (m.getImplementations().containsKey(interfaceOfBean))
                    beanInterface = interfaceOfBean;
                if (beanInterface != null) {
                    module = m;
                    break;
                }
            }
            if (module == null)
                continue;
            return module.save(t, beanInterface, sessionFactory, updateOptions, this);
        }
        throw new RuntimeException("unknown d.data repository for " + t.getClass().getName());
    }

    public <T extends Serializable> T save(T t) {
        return save(t, UpdateOptions.build().includeJsonProps().includeXmlProps());
    }


    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    public <T extends Serializable, C extends Serializable> DDataRepository<T, C> getBeanRepository(Class<T> beanClass) {
        Object r = buildedRepositories.get(beanClass);
        if (r != null) return (DDataRepository<T, C>) r;

        Class beanInterface = null;
        DDataModule module = null;
        for (Class interfaceOfBean : extractionAllInterfaces(beanClass)) {
            for (DDataModule m : modules.values()) {
                if (m.getImplementations().containsKey(interfaceOfBean))
                    beanInterface = interfaceOfBean;
                if(beanInterface!=null) {
                    module = m;
                    break;
                }
            }
            if (module != null)
                break;
        }
        if (module == null)
            throw new RuntimeException("unknown d.data repository for " + beanClass.getName());

        Class<DDataRepository<T, C>> repClass = module.getBeanRepositoryInterface(beanClass);
        r = buildedRepositories.get(repClass);
        if (r == null)
            buildedRepositories.put(beanClass, r = checkSingletonRepository(repClass,
                    module.getBeanRepository(beanClass, sessionFactory)));
        return (DDataRepository<T, C>) r;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> List<Class<? extends T>> getInterfaces4Supper(Class<T> superClass) {
        return modules.values().stream()
                .flatMap(m -> m.getImplementations().keySet().stream())
                .filter(superClass::isAssignableFrom)
                .filter(r -> r != superClass)
                .map(r -> (Class<? extends T>) r)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public <R> R getRepository(Class<R> clazz) {
        R r = (R) buildedRepositories.get(clazz);
        if (r != null) return r;

        for (DDataModule module : modules.values()) {
            r = module.getRepositoryByInterface(clazz, sessionFactory);
            if (r != null) {
                buildedRepositories.put(clazz, r = checkSingletonRepository(clazz, r));
                return r;
            }
        }
        throw new RuntimeException("unknown d.data repository " + clazz.getName());
    }

    @SuppressWarnings("unchecked")
    private synchronized <T, R extends T> R checkSingletonRepository(Class<T> clazz, R repository) {
        if (clazz == null) return null;

        R exists = (R) buildedRepositories.get(clazz);
        if (exists == null) {
            buildedRepositories.put(clazz, repository);
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
