package org.docero.data.example;

import org.docero.data.DDataProperty;
import org.docero.data.DDataVersionalBean;

import java.time.LocalDateTime;

public interface BuildingHEI extends DDataVersionalBean<LocalDateTime> {
    @DDataProperty(value = "datecreated", versionFrom = true, id = true)
    LocalDateTime getDateCreated();

    void setDateCreated(LocalDateTime dateCreated);
}
