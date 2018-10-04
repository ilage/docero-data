package org.docero.data.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"JavaReflectionMemberAccess", "unchecked"})
public class CachingRemoteRepository implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CachingRemoteRepository.class);
    private final DDataRemoteRepository target;
    private final Method callVersion;
    private final Method callList;
    private final Method getId;

    private final ConcurrentHashMap localCache = new ConcurrentHashMap<>();
    private final AtomicReference list = new AtomicReference<>(null);
    private int version = 0;
    private volatile long timeStamp = 0;

    public CachingRemoteRepository(DDataRemoteRepository target, Class type) {
        this.target = target;
        // store methods information for
        Method callVersion_ = null;
        Method callList_ = null;
        if (target instanceof DDataRemoteDictionary) {
            try {
                callVersion_ = target.getClass().getMethod("version_");
            } catch (NoSuchMethodException ignore) {
            }
            try {
                callList_ = target.getClass().getMethod("list");
            } catch (NoSuchMethodException ignore) {
            }
        }
        this.callVersion = callVersion_;
        this.callList = callList_;
        //
        Method getId_ = null;
        try {
            getId_ = type.getMethod("getDDataBeanKey_");
        } catch (NoSuchMethodException ignore) {
        }
        this.getId = getId_;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        LOG.trace("called '" + method.getName() + "' with " + (args == null ? 0 : args.length) + " arguments");
        Method m;
        Class<?>[] a;
        a = args == null || args.length == 0 ? null : new Class<?>[args.length];
        if (a != null) for (int i = 0; i < a.length; i++) a[i] = args[i].getClass();
        m = target.getClass().getMethod(method.getName(), a);

        if ("get".equals(m.getName()) && args != null && args.length == 1) {
            Object bean = localCache.get(args[0]);
            if (notModifiedInRepository() && bean != null) {
                LOG.trace("return '" + method.getName() + "' from local cache, for " + args[0]);
                return bean;
            } else {
                LOG.trace("return '" + method.getName() + "' from remote, for " + args[0]);
                if (getId != null) {
                    storeList();
                    bean = localCache.get(args[0]);
                } else {
                    bean = m.invoke(target, args);
                    localCache.put(args[0], bean);
                }
                return bean;
            }
        } else if ("list".equals(m.getName()) && a == null) {
            if (notModifiedInRepository() && list.get() != null) {
                LOG.trace("return '" + method.getName() + "' from local cache");
                return list.get();
            } else {
                LOG.trace("return '" + method.getName() + "' from remote");
                storeList();
                return list.get();
            }
        }

        LOG.trace("return '" + method.getName() + "' no caching");
        return m.invoke(target, args);
    }

    private void storeList() throws InvocationTargetException, IllegalAccessException {
        Integer version_ = (Integer) callVersion.invoke(target);
        timeStamp = System.currentTimeMillis();
        synchronized (localCache) {
            if (version == 0 || !Objects.equals(version, version_)) {
                version = version_;
                Object beanList = callList.invoke(target);
                list.set(beanList);
                if (getId != null) {
                    for (Object o : ((Collection) beanList))
                        localCache.put(getId.invoke(o), o);
                }
            }
        }
    }

    private boolean notModifiedInRepository() throws InvocationTargetException, IllegalAccessException {
        if (timeStamp > (System.currentTimeMillis() - 600)) {
            return true;
        } else if (callVersion != null) {
            Integer version_ = (Integer) callVersion.invoke(target);
            timeStamp = System.currentTimeMillis();
            boolean notModified;
            synchronized (localCache) {
                notModified = version != 0 && Objects.equals(version, version_);
                if (!notModified) localCache.clear();
            }
            return notModified;
        }
        return false;
    }
}
