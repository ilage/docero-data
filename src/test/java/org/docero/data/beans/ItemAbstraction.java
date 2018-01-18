package org.docero.data.beans;

import org.docero.data.DDataProperty;

import java.io.Serializable;

public interface ItemAbstraction extends Serializable {
    @DDataProperty(id = true)
    int getId();
    void setId(int value);

    @DDataProperty(discriminator = true)
    int getElemType();
    void setElemType(int value);

    @DDataProperty
    int getLinked();
    void setLinked(int value);
}
