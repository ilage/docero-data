package org.docero.data;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.temporal.Temporal;

public interface DDataVersionalRepository<T extends DDataVersionalBean<A>, C extends Serializable, A extends Temporal>
        extends DDataRepository<T, C> {
    T get(C id, A at);
}
