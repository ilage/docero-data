package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;

import java.io.Serializable;
import java.time.LocalDateTime;

@DDataBean(value = "h2", table = "h2", schema = "ddata")
public interface CompositeKeyInner extends Serializable {
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
}
