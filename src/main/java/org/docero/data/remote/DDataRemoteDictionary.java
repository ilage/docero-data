package org.docero.data.remote;

import org.docero.data.DictionaryType;

import javax.xml.bind.annotation.XmlTransient;
import java.util.List;

@XmlTransient
public interface DDataRemoteDictionary<T, C>
        extends DDataRemoteRepository<T, C> {
    DictionaryType getDictionaryType();
    Integer version_();
    List<T> list();
}
