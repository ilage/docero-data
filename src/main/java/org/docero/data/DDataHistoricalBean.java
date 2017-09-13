package org.docero.data;

import java.io.Serializable;
import java.time.temporal.Temporal;

public interface DDataHistoricalBean<A extends Temporal> extends Serializable {
    A actualFrom();
    void setActualFrom(A at);
}
