package org.docero.data.repositories;

import org.docero.data.DDataBatchOpsRepository;
import org.docero.data.DDataRep;
import org.docero.data.beans.Inner;
import org.docero.data.beans.Sample;

@DDataRep(beans = {
        Sample.class,
        Inner.class
})
public interface BatchRepository extends DDataBatchOpsRepository {
    //void testUnsupportedMethod(int i);
}
