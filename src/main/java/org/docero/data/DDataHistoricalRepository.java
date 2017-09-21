package org.docero.data;

import java.io.Serializable;
import java.time.temporal.Temporal;

public interface DDataHistoricalRepository<T extends DDataHistoricalBean<A>, C extends Serializable, A extends Temporal> extends DDataRepository<T,C> {
    T get(C id, A at);
}
