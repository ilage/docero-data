package org.docero.data.utils;

import java.io.Serializable;
import java.util.Map;

public interface DDataAttribute extends DDataBasicAttribute {
    String joinTable();
    String[] joinBy();
    String[] joinOn();
    String readExpression();
    String writeExpression();
    Class<? extends Serializable> getBeanInterface();
}
