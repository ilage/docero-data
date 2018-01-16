package org.docero.data.utils;

import java.util.Map;

public interface DDataAttribute {
    String getColumnName();
    String getPropertyName();
    Class getJavaType();
    String getJdbcType();
    boolean isDictionary();
    boolean isMappedBean();
    boolean isCollection();
    boolean isPrimaryKey();
    String joinTable();
    Map<String,String> joinMapping();
}
