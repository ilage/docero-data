package org.docero.data.utils;

import java.io.Serializable;
import java.util.Map;

public interface DDataBasicAttribute {
    String getColumnName();
    String getPropertyName();
    Class getJavaType();
    String getJdbcType();
    boolean isDictionary();
    boolean isMappedBean();
    boolean isCollection();
    boolean isPrimaryKey();
}
