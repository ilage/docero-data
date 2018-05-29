package org.docero.data.view;

import org.docero.data.utils.DSQL;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.docero.data.utils.DDataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
abstract class AbstractDataView {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataView.class);
    private static final String PROP_PATCH_DELIMITER = ".";
    private final AtomicInteger tablesCounter = new AtomicInteger(0);
    private final Map<String, CollectionColumn> subSelectsForColumns = new HashMap<>();

    abstract Temporal version();

    protected final List<DDataAttribute> rootAttributes = new ArrayList<>();
    protected DDataAttribute rootVersionFrom;

    private String rootTable;
    private String keyType;
    private HashSet<Integer> joinedInRootQuery;
    private HashSet<String> columnsInRoot;
    private final HashMap<String, JoinedTable> allJoins = new HashMap();

    DSQL buildFrom(Class[] roots) throws DDataException {
        try {
            rootTable = (String) roots[0].getDeclaredField("TABLE_NAME").get(null);
            allJoins.clear();
            allJoins.put("", new JoinedTable(0, 0, null));
            joinedInRootQuery = new HashSet<>();
            columnsInRoot = new HashSet<>();
            return new DSQL() {{
                FROM(rootTable + " as t0");
            }};
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new DDataException("select view not from *_WB_ enum: " + roots[0].getCanonicalName());
        }
    }

    String getKeySQL() {
        ArrayList<DDataAttribute> beanKeys = new ArrayList<>();
        String versionColumn = "";
        if (rootVersionFrom != null)
            versionColumn = rootVersionFrom.getColumnName();
        for (DDataAttribute a : rootAttributes)
            if (a.isPrimaryKey() && !versionColumn.equals(a.getColumnName())) beanKeys.add(a);

        if (beanKeys.size() == 1) {
            keyType = beanKeys.get(0).getJdbcType();
            return "t0.\"" + beanKeys.get(0).getColumnName() + "\"";
        } else {
            keyType = "VARCHAR";
            return "concat(" + beanKeys.stream()
                    .map(a -> "CAST(t0.\"" + a.getColumnName() + "\" TO VARCHAR)")
                    .collect(Collectors.joining(",\":\",")) + ")";
        }
    }

    void addColumnToViewSql(
            DSQL sql, DDataFilter column
    ) {
        addColumnToViewSql(sql, null,
                column, "", "", 0,
                joinedInRootQuery, columnsInRoot);
    }

    private void addColumnToViewSql(
            DSQL sql, DDataAttribute parentAttribute,
            DDataFilter column, String path, String uniqPath, int fromTableIndex,
            HashSet<Integer> alreadyJoined, HashSet<String> columnsInSelect
    ) {
        List<DDataAttribute> classAttrubutes = parentAttribute == null ?
                rootAttributes :
                Arrays.asList(((Class<? extends DDataAttribute>)
                        parentAttribute.getJavaType()).getEnumConstants());

        DDataAttribute attribute = null;
        for (DDataAttribute a : classAttrubutes)
            if (a.getPropertyName() != null && a.getPropertyName().equals(column.getName())) {
                attribute = a;
                break;
            }

        if (attribute != null && attribute.getColumnName() != null) {
            String pathAttributeName = path + attribute.getPropertyName();
            String pathAttributeKey = pathAttributeName + PROP_PATCH_DELIMITER;
            String uniqKey = uniqPath + attribute.getPropertyName() + ":" +
                    attribute.getJavaType().getSimpleName() + PROP_PATCH_DELIMITER;

            if (attribute.isMappedBean()) {
                JoinedTable table = allJoins.get(uniqKey);
                if (table == null) {
                    table = new JoinedTable(fromTableIndex, tablesCounter.incrementAndGet(), attribute);
                    allJoins.put(uniqKey, table);
                }

                if (!attribute.isCollection() && column.hasFilters()) {
                    if (!alreadyJoined.contains(table.tableIndex)) {
                        sql.LEFT_OUTER_JOIN(table.joinSql);
                        alreadyJoined.add(table.tableIndex);
                    }
                    for (DDataFilter col : column.getFilters()) {
                        addColumnToViewSql(sql, attribute,
                                col, pathAttributeKey, uniqKey,
                                table.tableIndex, alreadyJoined, columnsInSelect);

                        if (col.getAttribute().isMappedBean()) {
                            // add non id mapping columns used by col.attribute.joinMapping.keySet

                            for (String columnName : col.getAttribute().joinMapping().keySet()) {
                                DDataAttribute mapAttr = getNotIdAttrubuteByColumnName(attribute.getJavaType(), columnName);
                                if (mapAttr != null) {
                                    String mapKey = pathAttributeKey + mapAttr.getPropertyName();
                                    if (!columnsInSelect.contains(mapKey))
                                        try {
                                            addColumnToViewSql(sql, attribute,
                                                    new DDataFilter(mapAttr),
                                                    pathAttributeKey, uniqKey, table.tableIndex,
                                                    alreadyJoined, columnsInSelect);
                                            columnsInSelect.add(mapKey);
                                        } catch (DDataException ignore) {
                                        }
                                }
                            }
                        }
                    }
                    //add ids if not present in columnsInSelect
                    for (Field a : attribute.getJavaType().getDeclaredFields())
                        if (a.isEnumConstant()) {
                            DDataAttribute idAttribute = (DDataAttribute)
                                    Enum.valueOf(attribute.getJavaType(), a.getName());
                            if (idAttribute.isPrimaryKey()) {
                                String idKey = pathAttributeKey + idAttribute.getPropertyName();
                                if (!columnsInSelect.contains(idKey))
                                    try {
                                        addColumnToViewSql(sql, attribute,
                                                new DDataFilter(idAttribute),
                                                pathAttributeKey, uniqKey, table.tableIndex,
                                                alreadyJoined, columnsInSelect);
                                        columnsInSelect.add(idKey);
                                    } catch (DDataException ignore) {
                                    }
                            }
                        }
                } else if (column.getOperator() != null && column.getOperator().isAggregation()) {
                    if (!alreadyJoined.contains(table.tableIndex)) {
                        sql.LEFT_OUTER_JOIN(table.joinSql);
                        alreadyJoined.add(table.tableIndex);
                    }
                    sql.SELECT(column.getOperator() + "(t" + table.tableIndex + ".*)" +
                            " AS \"" + pathAttributeName + "\"");
                } else if (column.hasFilters()) { // is collection without aggregation
                    CollectionColumn cc = subSelectsForColumns.get(uniqKey);
                    if (cc == null) {
                        subSelectsForColumns.put(uniqKey, cc = new CollectionColumn(
                                table, pathAttributeKey, uniqKey, attribute));
                    }
                    cc.filters.addAll(column.getFilters());
                }
                // add non id mapping columns used by parentAttribute.joinMapping.values
                if (parentAttribute != null)
                    for (String columnName : parentAttribute.joinMapping().values()) {
                        DDataAttribute mapAttr = getNotIdAttrubuteByColumnName(attribute.getJavaType(), columnName);
                        if (mapAttr != null) {
                            String mapKey = pathAttributeKey + mapAttr.getPropertyName();
                            if (!columnsInSelect.contains(mapKey))
                                try {
                                    addColumnToViewSql(sql, attribute,
                                            new DDataFilter(mapAttr),
                                            pathAttributeKey, uniqKey, table.tableIndex,
                                            alreadyJoined, columnsInSelect);
                                    columnsInSelect.add(mapKey);
                                } catch (DDataException ignore) {
                                }
                        }
                    }
            } else if (column.getOperator() != null) {
                addFilterSql(sql, column, column.getAttribute().getClass(),
                        uniqPath, fromTableIndex, alreadyJoined);
            } else {
                if (!columnsInSelect.contains(pathAttributeName)) {
                    columnsInSelect.add(pathAttributeName);
                    String val = "t" + fromTableIndex + ".\"" + attribute.getColumnName() + "\"";
                    sql.SELECT(val + " AS \"" + pathAttributeName + "\"");
                    if (column.isSortAscending() != null)
                        sql.ORDER_BY(val + (column.isSortAscending() ? " ASC" : " DESC"));
                }
            }
        }
    }

    private DDataAttribute getNotIdAttrubuteByColumnName(Class<? extends DDataAttribute> clazz, String columnName) {
        return Arrays.stream(clazz.getEnumConstants())
                .filter(a -> a.getColumnName() != null)
                .filter(a -> !a.isPrimaryKey())
                .filter(a -> a.getColumnName().equals(columnName))
                .findAny().orElse(null);
    }

    List<DSQL> getSubSelects() {
        ArrayList<DSQL> subSelects = new ArrayList();
        for (CollectionColumn column : subSelectsForColumns.values()) {
            DSQL sql = new DSQL();
            sql.FROM(rootTable + " as t0");
            HashSet<Integer> joinedInSubQuery = new HashSet<>();
            HashSet<String> columnsInSubQuery = new HashSet<>();
            addJoinForSubSelects(sql, column.table, joinedInSubQuery);

            sql.SELECT(getKeySQL() + " as \"dDataBeanKey_\"");
            for (DDataFilter col : column.filters) {
                String uniqKey = column.uniqPath + col.getAttribute().getPropertyName() + ":" +
                        col.getAttribute().getJavaType().getSimpleName() + PROP_PATCH_DELIMITER;
                JoinedTable jt = allJoins.get(uniqKey);
                if (jt != null) addJoinForSubSelects(sql, jt, joinedInSubQuery);

                addColumnToViewSql(sql, column.byAttribute, col, column.path, column.uniqPath, column.table.tableIndex,
                        joinedInSubQuery, columnsInSubQuery);
            }
            //add ids if not present in columnsInSubQuery (setSortAscending(true))
            for (Field a : column.clazz.getDeclaredFields())
                if (a.isEnumConstant()) {
                    DDataAttribute idAttribute = (DDataAttribute)
                            Enum.valueOf(column.clazz, a.getName());
                    if (idAttribute.isPrimaryKey()) {
                        String idKey = column.path + idAttribute.getPropertyName();
                        if (!columnsInSubQuery.contains(idKey))
                            try {
                                addColumnToViewSql(sql, column.byAttribute, new DDataFilter(idAttribute) {{
                                            this.setSortAscending(true);
                                        }}, column.path, column.uniqPath, column.table.tableIndex,
                                        joinedInSubQuery, columnsInSubQuery);
                                columnsInSubQuery.add(idKey);
                            } catch (DDataException ignore) {
                            }
                    }
                }
            // add non id mapping columns used by column.byAttribute.joinMapping.values
            for (String columnName : column.byAttribute.joinMapping().values()) {
                DDataAttribute mapAttr = getNotIdAttrubuteByColumnName(column.clazz, columnName);
                if (mapAttr != null) {
                    String mapKey = column.path + mapAttr.getPropertyName();
                    if (!columnsInSubQuery.contains(mapKey))
                        try {
                            addColumnToViewSql(sql, column.byAttribute,
                                    new DDataFilter(mapAttr),
                                    column.path, column.uniqPath, column.table.tableIndex,
                                    joinedInSubQuery, columnsInSubQuery);
                            columnsInSubQuery.add(mapKey);
                        } catch (DDataException ignore) {
                        }
                }
            }
            subSelects.add(sql);
        }
        return subSelects;
    }

    private void addJoinForSubSelects(DSQL csql, JoinedTable table, HashSet<Integer> alreadyJoined) {
        if (table.fromTableIndex != 0) {
            for (JoinedTable joinedTable : allJoins.values())
                if (table.fromTableIndex == joinedTable.tableIndex) {
                    if (!alreadyJoined.contains(joinedTable.tableIndex))
                        addJoinForSubSelects(csql, joinedTable, alreadyJoined);
                    break;
                }
        }
        if (!alreadyJoined.contains(table.tableIndex)) {
            alreadyJoined.add(table.tableIndex);
            csql.LEFT_OUTER_JOIN(table.joinSql);
        }
    }

    /**
     * Check if filter can be applied for bean class
     *
     * @param clazz  *_WB_ enum tests for reference
     * @param column view columns reference
     * @return true if filter can be applied to bean class
     */
    boolean isApplicable(Class clazz, DDataFilter column) {
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isEnumConstant()) {
                DDataAttribute attribute = (DDataAttribute) Enum.valueOf(clazz, field.getName());
                String propertyName = attribute.getPropertyName();
                if (propertyName != null && column.getName().equals(propertyName)) {
                    return !column.hasFilters() ||
                            column.getFilters().stream().allMatch(c -> isApplicable(attribute.getJavaType(), c));
                }
            }
        }
        return false;
    }



    void addFilterSql(DSQL sql, DDataFilter rootFilter, Class rootClass) {
        addFilterSql(sql, rootFilter, rootClass, "", 0, joinedInRootQuery);
    }

    private void addFilterSql(DSQL sql, DDataFilter rootFilter, Class rootClass, String path, final int fromTableIndex, HashSet<Integer> alreadyJoined) {
        if (rootFilter == null) return;

        JoinedTable table = allJoins.get(path);
        if (table == null) {
            table = new JoinedTable(fromTableIndex, tablesCounter.incrementAndGet(), rootFilter.getAttribute());
            if (!alreadyJoined.contains(table.tableIndex)) {
                sql.LEFT_OUTER_JOIN(table.joinSql);
                alreadyJoined.add(table.tableIndex);
            }
            allJoins.put(path, table);
        }

        List<DDataFilter> appliedFilters = rootFilter.getFilters() == null ?
                Collections.singletonList(rootFilter) :
                rootFilter.getFilters().stream()
                        .filter(f -> isApplicable(rootClass, f))
                        .collect(Collectors.toList());

        for (DDataFilter filter : appliedFilters) {
            if (!filter.getAttribute().isCollection()) {
                if (filter.getOperator() != null && !filter.hasFilters()) {
                    String columnReference = "t" + table.tableIndex + ".\"" +
                            filter.getAttribute().getColumnName() + "\" ";
                    String columnType = filter.getAttribute().getJdbcType();
                    String condition;
                    switch (filter.getOperator().getOperands()) {
                        case 0:
                            condition = columnReference + filter.getOperator().toString();
                            break;
                        case 1:
                            String value;
                            if (filter.getOperator() == DDataFilterOperator.IN) {
                                if (filter.getValue().getClass().isArray()) {
                                    value = "(" + Arrays.stream((Object[]) filter.getValue())
                                            .map(Object::toString)
                                            .map(v -> DDataTypes.maskedValue(columnType, v))
                                            .collect(Collectors.joining(",")) + ")";
                                } else if (filter.getValue() instanceof Collection) {
                                    value = "(" + ((Collection<Object>) filter.getValue()).stream()
                                            .map(Object::toString)
                                            .map(v -> DDataTypes.maskedValue(columnType, v))
                                            .collect(Collectors.joining(",")) + ")";
                                } else
                                    value = "(" + DDataTypes.maskedValue(columnType, filter.getValue().toString()) + ")";
                            } else {
                                value = filter.getValue().toString();
                                if (filter.getOperator() == DDataFilterOperator.LIKE) value = "%" + value + "%";
                                else if (filter.getOperator() == DDataFilterOperator.NOT_LIKE)
                                    value = "%" + value + "%";
                                else if (filter.getOperator() == DDataFilterOperator.STARTS) value = value + "%";
                                else if (filter.getOperator() == DDataFilterOperator.NOT_STARTS) value = value + "%";
                            }
                            condition = columnReference + filter.getOperator().toString() + " " +
                                    DDataTypes.maskedValue(columnType, value);
                            break;
                        case 2: //BETWEEN only
                            condition = "(" + columnReference + "> " +
                                    DDataTypes.maskedValue(columnType, filter.getValue().toString()) +
                                    " AND " + columnReference + "< " +
                                    DDataTypes.maskedValue(columnType, filter.getValueTo().toString()) +
                                    ")";
                            break;
                        default:
                            condition = null;
                    }
                    if (condition != null) {
                        if (filter.getOperator().isAllowsNull())
                            condition = "(" + condition + " OR " + columnReference + "IS NULL)";
                        sql.WHERE(condition);
                    }
                } else if (filter.hasFilters()) {
                    Class innerClass = filter.getAttribute().getJavaType();
                    String key = path + filter.getAttribute().getPropertyName() + ":" +
                            innerClass.getSimpleName() + PROP_PATCH_DELIMITER;
                    addFilterSql(sql, filter, innerClass, key, table.tableIndex, alreadyJoined);
                }
            } else { // we can't use two separate filters by same table
                Class innerClass = filter.getAttribute().getJavaType();
                String key = path + filter.getAttribute().getPropertyName() + ":" +
                        innerClass.getSimpleName() + PROP_PATCH_DELIMITER;
                int finalN = tablesCounter.incrementAndGet();
                allJoins.put(key, new JoinedTable(fromTableIndex, finalN, filter.getAttribute()));
                sql.WHERE("EXISTS(" + new DSQL() {{
                    SELECT("*");
                    FROM(filter.getAttribute().joinTable() + " t" + finalN);
                    WHERE(filter.getAttribute().joinMapping().entrySet().stream()
                            .map(m -> "t" + fromTableIndex + "." + m.getKey() + "=t" + finalN + "." + m.getValue())
                            .collect(Collectors.joining(" AND ")));
                    String verSql = versionAndTypeConstraint(filter.getAttribute().getJavaType(), finalN);
                    if (verSql.length() > 0) WHERE(verSql);
                    addFilterSql(this, filter, innerClass, key, finalN, alreadyJoined);
                }}.toString() + ")");
            }
        }
    }

    private final static DateTimeFormatter sqlTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    String versionConstraint(Class clazz, int toTableIndex) {
        DDataAttribute versionFrom = null;
        DDataAttribute versionTo = null;
        try {
            versionFrom = (DDataAttribute) clazz.getDeclaredField("VERSION_FROM").get(null);
            versionTo = (DDataAttribute) clazz.getDeclaredField("VERSION_TO").get(null);
        } catch (IllegalAccessException | NoSuchFieldException ignore) {
        }
        String sql;
        if (versionFrom != null && versionTo != null) {
            if (version() == null) {
                sql = "t" + toTableIndex + ".\"" + versionTo.getColumnName() + "\" IS NULL";
            } else {
                String timeSql = "CAST ('" + sqlTimestamp.format(version()) + "' AS TIMESTAMP)";
                sql = "(t" + toTableIndex + ".\"" +
                        versionFrom.getColumnName() + "\" <= " + timeSql +
                        " AND (t" + toTableIndex + ".\"" + versionTo.getColumnName() +
                        "\" > " + timeSql + " OR " +
                        "t" + toTableIndex + ".\"" + versionTo.getColumnName() + "\" IS NULL))";
            }
        } else
            sql = "";

        return sql;
    }

    String typeConstraint(Class clazz, int toTableIndex) {
        DDataAttribute discriminant = null;
        String discriminantValue = null;
        try {
            discriminant = (DDataAttribute) clazz.getDeclaredField("DISCR_ATTR").get(null);
            discriminantValue = (String) clazz.getDeclaredField("DISCR_VAL").get(null);
        } catch (IllegalAccessException | NoSuchFieldException ignore) {
        }

        if (discriminant != null)
            return "t" + toTableIndex + ".\"" + discriminant.getColumnName() + "\"=" +
                    (String.class.isAssignableFrom(discriminant.getJavaType()) ?
                            "'" + discriminantValue + "'" :
                            discriminantValue);
        else
            return "";
    }

    private String versionAndTypeConstraint(Class clazz, int toTableIndex) {
        String sql = versionConstraint(clazz, toTableIndex);
        String tsql = typeConstraint(clazz, toTableIndex);
        sql = sql + (sql.length() > 0 && tsql.length() > 0 ? " AND " : "") + tsql;
        return sql;
    }

    public String getKeyType() {
        return keyType;
    }

    void addRootIdsToViewSql(DSQL sql) {
        for (DDataAttribute idAttribute : rootAttributes)
            if (idAttribute.isPrimaryKey()) {
                String idKey = idAttribute.getPropertyName();
                if (!columnsInRoot.contains(idKey))
                    try {
                        addColumnToViewSql(sql, null, new DDataFilter(idAttribute) {{
                                    this.setSortAscending(true);
                                }}, "", "", 0,
                                joinedInRootQuery, columnsInRoot);
                        columnsInRoot.add(idKey);
                    } catch (DDataException ignore) {
                    }
            }
    }

    private class JoinedTable {
        private final int fromTableIndex;
        private final int tableIndex;
        private final String joinSql;

        private JoinedTable(int fromTableIndex, int tableIndex, DDataAttribute attribute) {
            this.fromTableIndex = fromTableIndex;
            this.tableIndex = tableIndex;
            if (attribute != null) {
                String joinSql = attribute.joinTable() + " t" + tableIndex + " ON (" +
                        attribute.joinMapping().entrySet().stream()
                                .map(m -> "t" + fromTableIndex + "." + m.getKey() +
                                        "=t" + tableIndex + "." + m.getValue())
                                .collect(Collectors.joining(" AND "));
                String verSql = versionAndTypeConstraint(attribute.getJavaType(), tableIndex);
                this.joinSql = joinSql + (verSql.length() > 0 ? " AND " + verSql : "") + ")";
            } else
                this.joinSql = null;
        }
    }

    private class CollectionColumn {
        private final JoinedTable table;
        private final List<DDataFilter> filters = new ArrayList<>();
        private final String path;
        private final String uniqPath;
        private final Class clazz;
        private final DDataAttribute byAttribute;

        private CollectionColumn(JoinedTable table, String path, String uniqPath, DDataAttribute byAttribute) {
            this.table = table;
            this.path = path;
            this.uniqPath = uniqPath;
            this.byAttribute = byAttribute;
            this.clazz = byAttribute.getJavaType();
        }
    }
}
