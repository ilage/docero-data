package org.docero.data.utils;

public interface DDataAttribute {
    String getColumnName();
    String getPropertyName();
    Class getJavaType();
    String getJdbcType();
}
