package org.docero.data.utils;

public interface DDataBasicAttribute {
    /**
     * @return column name of table
     */
    String getColumnName();
    /**
     * @return property name of bean
     */
    String getPropertyName();
    /**
     * @return java class of property or enumeration of associated ddata-bean
     */
    Class getJavaType();
    /**
     * @return SQL type of column
     */
    String getJdbcType();
    boolean isDictionary();
    boolean isMappedBean();
    boolean isCollection();
    boolean isPrimaryKey();
    boolean isNullable();
}
