package org.docero.data;

import java.io.Serializable;

public interface DDataRemoteRepository<T extends Serializable, C extends Serializable> {
    T get(C id);
    T insert(T bean);
    T update(T bean);
    void delete(C id);
}
