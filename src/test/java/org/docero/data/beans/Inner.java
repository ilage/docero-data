package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;
import org.docero.data.DictionaryType;
import org.docero.data.GeneratedValue;

import java.io.Serializable;

@DDataBean(value = "inner", table = "inner", schema = "ddata")
public interface Inner extends Serializable {
    @GeneratedValue("ddata.sample_seq")
    @DDataProperty(value = "id", id = true)
    int getId();
    void setId(int id);

    //it's a default: @IInner_Map_(value = Inner_.SAMPLE, sample = Sample_.ID)
    @DDataProperty("sample_id")
    int getSampleId();
    void setSampleId(int id);

    @DDataProperty
    @Inner_Map_(value = Inner_.SAMPLE_ID, sample = Sample_.ID, markTransient = true)
    Sample getSample();
    void setSample(Sample bean);

    @DDataProperty("text")
    String getText();
    void setText(String text);
}
