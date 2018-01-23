package org.docero.data.repositories;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.Inner;
import org.docero.data.beans.Inner_;

import java.util.List;

@DDataRep
public interface InnerDictionary extends DDataRepository<Inner,Integer> {
    @InnerDictionary_DDataFetch_(forwardOrder={Inner_.TEXT})
    List<Inner> list();
}
