package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataVersionalBean;
import org.docero.data.DDataProperty;

import java.time.LocalDateTime;

@DDataBean(value = "h1", table = "h1", schema = "ddata")
public interface HistSample extends DDataVersionalBean<LocalDateTime> {
    @DDataProperty(value = "id", id = true)
    int getId();

    void setId(int id);

    @DDataProperty(value = "date_from", id = true, versionFrom = true)
    LocalDateTime getDateFrom();

    void setDateFrom(LocalDateTime dt);

    @DDataProperty(value = "date_to", versionTo = true)
    LocalDateTime getDateTo();

    void setDateTo(LocalDateTime dt);

    @DDataProperty(value = "s")
    String getValue();

    void setValue(String val);

    @DDataProperty("s2")
    Integer getNumeric();

    void setNumeric(Integer v);

    @DDataProperty(value = "inner")
    int getInnerId();

    void setInnerId(int id);

    @DDataProperty
    @HistSample_Map_(
            value = {HistSample_.INNER_ID},
            inner = {HistInner_.ID}
    )
    HistInner getInner();

    void setInner(HistInner bean);
}
