package org.docero.data.beans;

import org.docero.data.DDataBean;

@DDataBean(table = "a1", schema = "ddata")
public interface ItemInner extends ItemAbstraction {
    @ItemInner_Map_(value = ItemInner_.LINKED, inner = Inner_.ID)
    Inner getInner();
}
