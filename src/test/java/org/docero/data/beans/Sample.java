package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;
import org.docero.data.rmt.RemoteBean;

import java.util.List;

@DDataBean(value = "sample", table = "sample", schema = "ddata")
public interface Sample extends SampleTable {
    @DDataProperty
    @Sample_Map_(value = Sample_.INNER_ID, inner = Inner_.ID)
    Inner getInner();

    void setInner(Inner inner);

    @Sample_Map_(listParameter = Inner_.SAMPLE_ID)
    List<? extends Inner> getListParameter();

    void setListParameter(List<? extends Inner> list);

    @Sample_Map_(value = Sample_.ID, item = ItemAbstraction_.ID)
    ItemAbstraction getItem();

    @Sample_Map_(value = Sample_.ID)//, func = "get")
    RemoteBean getRemoteBean();
    //must generate error at compile: void setRemoteBean(RemoteBean v);
    /*possible solution: default void setRemoteBean(RemoteBean v) {
        this.setId(v.getRemoteId());
    }*/

    /**
     * @return unknown for ddata library property generated with simple getter and setter
     */
    /*List<RemoteBean> getAbstraction();

    void setAbstraction(List<RemoteBean> i);*/
}
