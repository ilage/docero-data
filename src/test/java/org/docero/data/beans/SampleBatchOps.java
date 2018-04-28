package org.docero.data.beans;

import org.docero.data.DDataBatchOpsRepository;
import org.docero.data.DDataRep;

@DDataRep(beans = {
        Sample.class, Inner.class
})
public interface SampleBatchOps extends DDataBatchOpsRepository {
    Sample getOneBy(String val, Integer listId);
}
