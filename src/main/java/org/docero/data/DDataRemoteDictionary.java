package org.docero.data;

import java.io.Serializable;
import java.util.List;

public interface DDataRemoteDictionary<T extends Serializable, C extends Serializable>
        extends DDataRemoteRepository<T, C> {
    DictionaryType getDictionaryType();
    Integer version_();
    List<T> list();
}
