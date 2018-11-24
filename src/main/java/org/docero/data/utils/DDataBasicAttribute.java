package org.docero.data.utils;

public interface DDataBasicAttribute {
    String getColumnName();
    String getPropertyName();
    Class getJavaType();
    String getJdbcType();
    boolean isDictionary();
    boolean isMappedBean();
    boolean isCollection();
    boolean isPrimaryKey();
    boolean isNullable();
}
