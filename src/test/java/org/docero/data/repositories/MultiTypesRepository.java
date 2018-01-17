package org.docero.data.repositories;

import org.docero.data.DDataDiscriminator;
import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.*;

import java.util.List;

@DDataRep(discriminator = {
        @DDataDiscriminator(value = "1",bean = ItemInner.class),
        @DDataDiscriminator(value = "2",bean = ItemSample.class),
        @DDataDiscriminator(value = "3", bean = ItemItemSample.class)
})
@MultiTypesRepository_Discriminator_(value = ItemAbstraction_.ELEM_TYPE)
public interface MultiTypesRepository extends DDataRepository<ItemAbstraction,Integer> {
    <T extends ItemAbstraction> List<T> list();
    @MultiTypesRepository_DDataFetch_(ignore = ItemAbstraction_.ELEM_TYPE)
    void update(ItemAbstraction obj);
}
