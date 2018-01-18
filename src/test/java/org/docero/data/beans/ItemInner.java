package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataDiscriminator;
import org.docero.data.DDataProperty;

@DDataBean(table = "a1", schema = "ddata")
@DDataDiscriminator("1")
public interface ItemInner extends ItemAbstraction {
    @DDataProperty("lg")
    Integer getLgId();
    @ItemInner_Map_(value = ItemInner_.LINKED, inner = Inner_.ID)
    Inner getInner();
    @ItemInner_Map_(value = ItemInner_.LG_ID, large = LargeNotDict_.ID)
    LargeNotDict getLarge();
    void setLarge(LargeNotDict val);
    @ItemInner_Map_(value = ItemInner_.LG_ID, largeCached = LargeDict_.ID)
    LargeDict getLargeCached();
    void setLargeCached(LargeDict val);
}
