package org.docero.data.rmt;

import java.io.Serializable;

public interface RBean extends Serializable {
    int getRemoteId();
    void setRemoteId(int v);

    String getName();
    void setName(String v);
}
