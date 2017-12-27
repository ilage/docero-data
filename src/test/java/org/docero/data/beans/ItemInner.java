package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;

@DDataBean(table = "a1", schema = "ddata")
public interface ItemInner extends ItemAbstraction {
    @DDataProperty("lg")
    Integer getLgId();
    void setLgId(Integer val);
    @ItemInner_Map_(value = ItemInner_.LINKED, inner = Inner_.ID)
    Inner getInner();
    @ItemInner_Map_(value = ItemInner_.LG_ID, large = LargeNotDict_.ID)
    LargeNotDict getLarge();
}
