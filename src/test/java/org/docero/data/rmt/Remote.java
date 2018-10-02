package org.docero.data.rmt;

import org.docero.data.remote.DDataPrototype;
import org.docero.data.remote.DDataPrototypeId;

import java.io.Serializable;

//@DDataPrototype
public interface Remote extends Serializable {
    @DDataPrototypeId
    int getRemoteId();
    void setRemoteId(int v);

    String getName();
    void setName(String v);
}
