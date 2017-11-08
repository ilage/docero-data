package org.docero.data.example;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;
import org.docero.data.GeneratedValue;

import java.io.Serializable;
import java.util.List;

@DDataBean(value = "sample", table = "sample", schema = "ddata")
public interface Sample extends Serializable {
    @GeneratedValue("ddata.sample_seq")
    @DDataProperty(value = "id", id = true)
    int getId();
    void setId(int id);

    @DDataProperty(value = "s", nullable = false)
    String getStrParameter();
    void setStrParameter(String p);

    @DDataProperty("i")
    int getInnerId();
    void setInnerId(int id);

    @DDataProperty
    @Sample_Map_(value = Sample_.INNER_ID,inner = Inner_.ID)
    Inner getInner();
    void setInner(Inner inner);

    @Sample_Map_(listParameter = Inner_.SAMPLE_ID)
    List<? extends Inner> getListParameter();
    void setListParameter(List<? extends Inner> list);
}
