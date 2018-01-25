package org.docero.data.repositories;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.Inner;
import org.docero.data.beans.Inner_;
import org.docero.data.beans.LargeDict;
import org.docero.data.beans.LargeDict_;
import org.docero.data.utils.DDataDictionary;

import java.util.List;

@DDataRep
public interface LargeDictionary extends DDataDictionary<LargeDict,Integer> {
    @LargeDictionary_DDataFetch_(forwardOrder={LargeDict_.NAME})
    List<LargeDict> list();
}
