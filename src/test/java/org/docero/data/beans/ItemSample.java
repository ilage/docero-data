package org.docero.data.beans;

import org.docero.data.DDataBean;

@DDataBean(table = "a1", schema = "ddata")
public interface ItemSample extends ItemAbstraction {
    @ItemSample_Map_(value = ItemSample_.LINKED, sample = Sample_.ID)
    Sample getSample();
}
