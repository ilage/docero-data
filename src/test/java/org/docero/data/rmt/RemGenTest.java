package org.docero.data.rmt;

import org.docero.data.remote.DDataPrototype;
import org.docero.data.remote.DDataPrototypeId;

@DDataPrototype
public interface RemGenTest {
    @DDataPrototypeId
    int getId();
    void setId(int v);

    String getName();
    void setName(String v);

    RemoteBean getBean();
}
