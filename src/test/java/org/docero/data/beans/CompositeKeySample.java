package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

@DDataBean(value = "h1", table = "h1", schema = "ddata")
public interface CompositeKeySample extends Serializable {
    @DDataProperty(value = "id", id = true)
    int getId();

    void setId(int id);

    @DDataProperty(value = "date_from", id = true)
    LocalDateTime getDateFrom();

    void setDateFrom(LocalDateTime dt);

    @DDataProperty(value = "date_to")
    LocalDateTime getDateTo();

    void setDateTo(LocalDateTime dt);

    @DDataProperty(value = "s")
    String getValue();

    void setValue(String val);

    @DDataProperty(value = "inner")
    Integer getInnerId();

    void setInnerId(Integer id);

    @DDataProperty
    @CompositeKeySample_Map_(
            value = {CompositeKeySample_.INNER_ID, CompositeKeySample_.DATE_FROM},
            inner = {CompositeKeyInner_.ID, CompositeKeyInner_.DATE_FROM}
    )
    CompositeKeyInner getInner();

    void setInner(CompositeKeyInner bean);


    default LocalDate getLocalDate() {
        return getDateFrom().toLocalDate();
    }

    default void setLocalDate(LocalDate value) {
        setDateFrom(value.atStartOfDay());
    }
}
