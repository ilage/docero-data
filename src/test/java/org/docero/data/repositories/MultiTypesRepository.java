package org.docero.data.repositories;

import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.*;

import java.util.List;

@DDataRep(beans = {ItemInner.class, ItemSample.class, ItemItemSample.class})
public interface MultiTypesRepository extends DDataRepository<ItemAbstraction, Integer> {
    <T extends ItemAbstraction> List<T> list();

    @MultiTypesRepository_DDataFetch_(ignore = ItemAbstraction_.ELEM_TYPE)
    void update(ItemAbstraction obj);
}
