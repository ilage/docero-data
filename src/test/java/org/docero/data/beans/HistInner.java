package org.docero.data.beans;

import org.docero.data.*;

import java.io.Serializable;
import java.time.LocalDateTime;

@DDataBean(value = "h2", table = "h2", schema = "ddata")
public interface HistInner extends DDataVersionalBean<LocalDateTime> {
    @GeneratedValue("ddata.sample_seq")
    @DDataProperty(value = "id", id = true)
    int getId();
    void setId(int id);

    @DDataProperty(value = "date_from", id = true, versionFrom = true)
    LocalDateTime getDateFrom();

    void setDateFrom(LocalDateTime dt);

    @DDataProperty(value = "date_to", versionTo = true)
    LocalDateTime getDateTo();

    void setDateTo(LocalDateTime dt);

    @DDataProperty("s")
    String getText();
    void setText(String text);
}
