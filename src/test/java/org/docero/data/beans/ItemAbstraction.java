package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;

import java.io.Serializable;

@DDataBean(table = "a1", schema = "ddata")
public interface ItemAbstraction extends Serializable {
    @DDataProperty(id = true)
    int getId();
    void setId(int value);

    @DDataProperty
    int getElemType();
    void setElemType(int value);

    @DDataProperty
    int getLinked();
    void setLinked(int value);
}
