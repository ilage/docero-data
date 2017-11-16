package org.docero.data.utils;

import org.docero.data.DDataRepository;

import java.io.Serializable;
import java.util.List;

public interface DDataDictionary<T extends Serializable, C extends Serializable> extends DDataRepository<T, C> {
    <B extends T> B put_(B bean);
    void putList_(List<C> beans);
    List<T> list();
}
