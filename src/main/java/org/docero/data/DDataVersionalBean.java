package org.docero.data;

import java.io.Serializable;
import java.time.temporal.Temporal;

public interface DDataVersionalBean<A extends Temporal> extends Serializable {
    A getActualFrom_();
    void setActualFrom_(A at);
}
