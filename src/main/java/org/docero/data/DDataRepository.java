package org.docero.data;

import java.io.Serializable;

public interface DDataRepository<T extends Serializable, C extends Serializable> {
    <A extends T> A create();
    <A extends T> A get(C id);
    void insert(T bean);
    void update(T bean);
    void delete(C id);
}
