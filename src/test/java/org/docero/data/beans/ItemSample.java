package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataDiscriminator;
import org.docero.data.DDataProperty;

@DDataBean(table = "a1", schema = "ddata")
@DDataDiscriminator("2")
public interface ItemSample extends ItemAbstraction {
    @DDataProperty("sm")
    Integer getSmId();
    void setSmId(Integer val);
    @DDataProperty("lg")
    Integer getLgId();
    void setLgId(Integer val);
    @ItemSample_Map_(value = ItemSample_.LINKED, sample = Sample_.ID)
    Sample getSample();
    @ItemSample_Map_(value = ItemSample_.SM_ID, small = SmallDict_.ID)
    SmallDict getSmall();
    @ItemSample_Map_(value = ItemSample_.LG_ID, large = LargeNotDict_.ID)
    LargeNotDict getLarge();
}
