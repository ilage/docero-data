package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataDiscriminator;
import org.docero.data.DDataProperty;

@DDataBean(table = "a1", schema = "ddata")
@DDataDiscriminator("3")
public interface ItemItemSample extends ItemAbstraction {
    @DDataProperty("sm")
    Integer getSmId();
    void setSmId(Integer val);
    @DDataProperty("lg")
    Integer getLgId();
    void setLgId(Integer val);
    @ItemItemSample_Map_(value = ItemItemSample_.LINKED, sample = ItemSample_.ID)
    ItemSample getSample();
    @ItemItemSample_Map_(value = ItemItemSample_.SM_ID, small = SmallDict_.ID)
    SmallDict getSmall();
    @ItemItemSample_Map_(value = ItemItemSample_.LG_ID, large = LargeNotDict_.ID)
    LargeNotDict getLarge();
}
