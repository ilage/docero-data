package org.docero.data;

import java.io.Serializable;

public interface DDataRepository<T extends Serializable, C extends Serializable> {
    <A extends T> A create();
    <A extends T> A get(C id);
    <A extends T> A  insert(T bean);
    <A extends T> A  update(T bean);
    void delete(C id);
}
