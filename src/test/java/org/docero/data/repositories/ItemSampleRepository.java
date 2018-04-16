package org.docero.data.repositories;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.ItemSample;
import org.docero.data.beans.ItemSample_;

import java.util.List;

@DDataRep
public interface ItemSampleRepository extends DDataRepository<ItemSample,Integer> {
    @ItemSampleRepository_DDataFetch_(forwardOrder = ItemSample_.LG_ID)
    List<ItemSample> list();
}
