package org.docero.data.repositories;

import org.docero.data.DDataFetchType;
import org.docero.data.DDataRep;
import org.docero.data.DDataRepository;
import org.docero.data.beans.*;

import java.util.List;

@DDataRep("ck_samples")
public interface CompositeKeyRepository extends DDataRepository<CompositeKeySample, CompositeKeySample_Key_> {
    /*@CompositeKeyRepository_DDataFetch_(value = DDataFetchType.EAGER, eagerTrunkLevel = 2)
    CompositeKeySample get(CompositeKeySample_Key_ key);*/

    @CompositeKeyRepository_DDataFetch_(value = DDataFetchType.EAGER, eagerTrunkLevel = 2)
    List<CompositeKeySample> list(
            @CompositeKeyRepository_Filter_(inner = CompositeKeyInner_.ID)
            Integer innerId,
            @CompositeKeyRepository_Filter_(inner = CompositeKeyInner_.VALUE)
            String innerValue
    );
}
