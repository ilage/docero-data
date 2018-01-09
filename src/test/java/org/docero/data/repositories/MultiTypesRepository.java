package org.docero.data.repositories;

import org.docero.data.DDataDiscriminator;
import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.ItemAbstraction;
import org.docero.data.beans.ItemAbstraction_;
import org.docero.data.beans.ItemInner;
import org.docero.data.beans.ItemSample;

import java.util.List;

@DDataRep(discriminator = {
        @DDataDiscriminator(value = "1",bean = ItemInner.class),
        @DDataDiscriminator(value = "2",bean = ItemSample.class)
})
@MultiTypesRepository_Discriminator_(value = ItemAbstraction_.ELEM_TYPE)
public interface MultiTypesRepository extends DDataRepository<ItemAbstraction,Integer> {
    <T extends ItemAbstraction> List<T> list();

    void update(ItemAbstraction obj);
}
