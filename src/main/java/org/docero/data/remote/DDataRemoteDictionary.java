package org.docero.data.remote;

import org.docero.data.DictionaryType;
import org.docero.data.utils.DDataCachedLists;

import javax.xml.bind.annotation.XmlTransient;

@XmlTransient
public interface DDataRemoteDictionary<T, C> extends DDataRemoteRepository<T, C>, DDataCachedLists<T> {
    DictionaryType getDictionaryType();
}
