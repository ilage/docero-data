package org.docero.data.utils;

import java.util.List;

public interface DDataCachedLists<T> {
    Integer version_();
    List<T> list();
}
