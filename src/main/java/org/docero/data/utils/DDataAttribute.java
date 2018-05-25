package org.docero.data.utils;

import java.io.Serializable;
import java.util.Map;

public interface DDataAttribute extends DDataBasicAttribute {
    String joinTable();
    Map<String,String> joinMapping();
    Class<? extends Serializable> getBeanInterface();
}
