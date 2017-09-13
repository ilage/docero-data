package org.docero.data;

import java.io.Serializable;

public interface DDataRepository<T extends Serializable, C> {
    T create();
    T get(C id);
    void insert(T bean);
    void update(T bean);
    void delete(C id);
}
