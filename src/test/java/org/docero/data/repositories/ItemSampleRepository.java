package org.docero.data.repositories;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.ItemSample;

import java.util.List;

@DDataRep
public interface ItemSampleRepository extends DDataRepository<ItemSample,Integer> {
    List<ItemSample> list();
}
