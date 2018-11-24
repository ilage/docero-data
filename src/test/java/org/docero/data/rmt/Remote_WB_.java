package org.docero.data.rmt;

/*
    Class generated by docero-data processor.
*/
public enum Remote_WB_ implements org.docero.data.utils.DDataAttribute {
    /**
     * Value of column null
     */
    REMOTE_ID(null, "remoteId", java.lang.Integer.class, "INTEGER", false, false, false, null, null, true, null),
    /**
     * Value of column null
     */
    NAME(null, "name", java.lang.String.class, "VARCHAR", false, false, false, null, null, false, null),
    NONE_(null, null, null, null, false, false, false, null, null, false, null);

    public final static String TABLE_NAME = null;
    public final static Class<org.docero.data.rmt.Remote> BEAN_INTERFACE = org.docero.data.rmt.Remote.class;
    public final static org.docero.data.rmt.Remote_WB_ DISCR_ATTR = null;
    public final static String DISCR_VAL = null;
    public final static org.docero.data.rmt.Remote_WB_ VERSION_FROM = null;
    public final static org.docero.data.rmt.Remote_WB_ VERSION_TO = null;

    private final String columnName;
    private final String propertyName;
    private final Class javaType;
    private final Class<? extends java.io.Serializable> interfaceType;
    private final String jdbcType;
    private final boolean dictionary;
    private final boolean mapped;
    private final boolean collection;
    private final String joinTable;
    private final String[] joinBy;
    private final String[] joinOn;
    private final boolean isPrimaryKey;

    private Remote_WB_(String columnName, String propertyName, Class javaType, String jdbcType, boolean dictionary, boolean mapped, boolean collection, String joinTable, java.util.Map<String, String> joinMap, boolean isPrimaryKey, Class<? extends java.io.Serializable> interfaceType) {
        this.columnName = columnName;
        this.propertyName = propertyName;
        this.javaType = javaType;
        this.jdbcType = jdbcType;
        this.dictionary = dictionary;
        this.mapped = mapped;
        this.collection = collection;
        this.joinTable = joinTable;
        this.joinBy = null;
        this.joinOn = null;
        this.isPrimaryKey = isPrimaryKey;
        this.interfaceType = interfaceType;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public Class getJavaType() {
        return javaType;
    }

    @Override
    public String getJdbcType() {
        return jdbcType;
    }

    @Override
    public boolean isDictionary() {
        return dictionary;
    }

    @Override
    public boolean isMappedBean() {
        return mapped;
    }

    @Override
    public boolean isCollection() {
        return collection;
    }

    @Override
    public String joinTable() {
        return joinTable;
    }

    @Override
    public String[] joinBy() {
        return joinBy;
    }

    @Override
    public String[] joinOn() {
        return joinOn;
    }

    @Override
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @Override
    public boolean isNullable() {return true;}

    @Override
    public Class<? extends java.io.Serializable> getBeanInterface() {
        return interfaceType;
    }

    @Override
    public String readExpression() {
        return null;
    }

    @Override
    public String writeExpression() {
        return null;
    }
}
