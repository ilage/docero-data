package org.docero.data.view;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.DDataDictionariesService;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.docero.data.utils.DDataTypes;
import org.docero.data.utils.DSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("unchecked")
abstract class AbstractDataView {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataView.class);

    private static final String PROP_PATCH_DELIMITER = ".";
    private final AtomicInteger tablesCounter = new AtomicInteger(0);
    private final Map<String, CollectionJoin> selectsForCollections = new HashMap<>();

    private final TableEntity rootEntity;
    private final HashMap<String, TableEntity> tableEntities = new HashMap<>();
    private final DDataDictionariesService dictionariesService;
    final Class<? extends DDataAttribute>[] roots;
    final HashMap<String, TableCell> tableCells = new HashMap<>();

    private String keyType;
    private HashSet<Integer> joinedInRootQuery;
    private HashSet<String> columnsInRoot;
    private final HashMap<String, JoinedTable> allJoins = new HashMap();
    private final HashMap<String, List<RemoteBeanRef>> remoteBeans = new HashMap<>();

    abstract Temporal version();

    AbstractDataView(Class<? extends DDataAttribute>[] roots, DDataFilter[] columns, DDataDictionariesService dictionariesService) {
        this.roots = roots;
        this.dictionariesService = dictionariesService;

        rootEntity = new TableEntity(roots[0]);
        String versionColumn = rootEntity.versionFrom == null ? "" : rootEntity.versionFrom.getColumnName();

        for (Class<? extends DDataAttribute> root : roots)
            for (DDataAttribute attr : root.getEnumConstants())
                if (attr.getPropertyName() != null && rootEntity.attributes.stream()
                        .noneMatch(a -> a.getPropertyName().equals(attr.getPropertyName()) &&
                                        a.getJavaType().equals(attr.getJavaType()) && (
                                        (a.joinBy() == null && attr.joinBy() == null) ||
                                                (a.joinBy() != null && attr.joinBy() != null &&
                                                        Arrays.equals(a.joinBy(), attr.joinBy()))
                                )
                        )) {
                    rootEntity.attributes.add(attr);
                    if (attr.isPrimaryKey()) {
                        rootEntity.addCell(new TableCell(attr.getPropertyName(), attr,
                                versionColumn.equals(attr.getColumnName())));
                    }
                }
        tableEntities.put(null, rootEntity);

        fillViewEntities(Arrays.asList(columns), null, rootEntity);
    }

    private void fillViewEntities(List<DDataFilter> columns, String path, final TableEntity parent) {
        // basic values
        for (DDataFilter column : columns) {
            DDataAttribute attribute = column.getAttribute();
            String versionColumn = parent.versionFrom == null ? "" : parent.versionFrom.getColumnName();
            if (attribute != null && !attribute.isMappedBean()) {
                String nameInPath = column.getMapName();
                String cp = path == null ? nameInPath : (path + "." + nameInPath);
                parent.addCell(new TableCell(cp, column, versionColumn.equals(attribute.getColumnName())));
            }
        }
        // mapped beans
        for (DDataFilter column : columns) {
            DDataAttribute attribute = column.getAttribute();
            if (attribute != null && attribute.isMappedBean()) {
                String nameInPath = column.getMapName();
                String cp = path == null ? nameInPath : (path + "." + nameInPath);

                TableEntity entity = new TableEntity(parent, cp, column);
                parent.addEntity(entity);
                String versionColumn = entity.versionFrom == null ? "" : entity.versionFrom.getColumnName();

                if (!column.isExternalData())
                    for (DDataAttribute entityAttr : entity.attributes) {
                        int idx = indexOf(attribute.joinOn(), entityAttr.getColumnName());
                        String joinBy = idx < 0 ? null : attribute.joinBy()[idx];
                        if (entityAttr.isPrimaryKey() || joinBy != null) {
                            TableCell idCell = new TableCell(cp + "." + entityAttr.getPropertyName(), entityAttr,
                                    versionColumn.equals(entityAttr.getColumnName()));
                            entity.addCell(idCell);
                            if (joinBy != null) {
                                TableCell parentMapCell = parent.cells.stream()
                                        .filter(c -> joinBy.equals(c.attribute.getColumnName()))
                                        .findAny().orElse(null);
                                if (parentMapCell == null) {
                                    DDataAttribute parentMapAttr = parent.attributes.stream()
                                            .filter(c -> joinBy.equals(c.getColumnName()))
                                            .findAny().orElse(null);
                                    assert parentMapAttr != null;
                                    parent.addCell(parentMapCell = new TableCell(
                                            (path == null ? "" : path + ".") + parentMapAttr.getPropertyName(),
                                            parentMapAttr,
                                            false
                                    ));
                                }
                                parent.mappings.put(parentMapCell, idCell);
                            }
                        }
                    }
                else {
                    RemoteBeanRef rbr = new RemoteBeanRef(path, nameInPath, attribute.getBeanInterface(),
                            attribute.joinOn() == null ? null : attribute.joinOn()[0]);
                    for (DDataAttribute entityAttr : parent.attributes) {
                        int idx = indexOf(attribute.joinBy(), entityAttr.getColumnName());
                        String joinBy = idx < 0 ? null : attribute.joinBy()[idx];
                        if (joinBy != null) {
                            TableCell parentMapCell = parent.cells.stream()
                                    .filter(c -> joinBy.equals(c.attribute.getColumnName()))
                                    .findAny().orElseGet(() -> {
                                        TableCell pmc = new TableCell(
                                                (path == null ? "" : path + ".") + entityAttr.getPropertyName(),
                                                entityAttr,
                                                false);
                                        parent.addCell(pmc);
                                        return pmc;
                                    });
                            rbr.addParameter(parentMapCell.name);
                        }
                    }
                    remoteBeans.computeIfAbsent(path, k -> new ArrayList<>()).add(rbr);
                }

                if (!column.isExternalData() && column.getFilters() != null)
                    fillViewEntities(column.getFilters(), cp, entity);
            } else if (attribute == null && column.getFilters() != null)
                fillViewEntities(column.getFilters(), path, parent);
        }
    }

    private int indexOf(String[] names, String name) {
        if (names != null && name != null)
            for (int i = 0; i < names.length; i++) {
                if (Objects.equals(name, names[i])) return i;
            }
        return -1;
    }

    TableEntity getEntityForPath(String s) {
        return tableEntities.get(s);
    }

    DSQL buildFrom() {
        allJoins.clear();
        allJoins.put("", new JoinedTable(0, 0, null));
        joinedInRootQuery = new HashSet<>();
        columnsInRoot = new HashSet<>();
        return new DSQL() {{
            FROM(rootEntity.table + " as t0");
        }};
    }

    String getKeySQL() {
        ArrayList<DDataAttribute> beanKeys = new ArrayList<>();
        String versionColumn = "";
        if (rootEntity.versionFrom != null)
            versionColumn = rootEntity.versionFrom.getColumnName();
        for (DDataAttribute a : rootEntity.attributes)
            if (a.isPrimaryKey() && (selectAllVersions() || !versionColumn.equals(a.getColumnName())))
                beanKeys.add(a);

        if (beanKeys.size() == 1) {
            keyType = beanKeys.get(0).getJdbcType();
            return "t0.\"" + beanKeys.get(0).getColumnName() + "\"";
        } else {
            keyType = "VARCHAR";
            return "concat(" + beanKeys.stream()
                    .map(a -> "CAST(t0.\"" + a.getColumnName() + "\" AS VARCHAR)")
                    .collect(Collectors.joining(",':',")) + ")";
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
            final HashSet<Integer> alreadyJoined, final HashSet<String> columnsInSelect
    ) {
        List<DDataAttribute> classAttrubutes = parentAttribute == null ?
                rootEntity.attributes :
                Arrays.asList(((Class<? extends DDataAttribute>)
                        parentAttribute.getJavaType()).getEnumConstants());

        DDataAttribute attribute = null;
        for (DDataAttribute a : classAttrubutes)
            if (a.getPropertyName() != null && a.getPropertyName().equals(column.getName())) {
                attribute = a;
                break;
            }

        if (attribute != null) {
            final String pathAttributeName = path + column.getMapName();
            final String pathAttributeKey = pathAttributeName + PROP_PATCH_DELIMITER;
            final String uniqKey = uniqPath + column.getMapName() + ":" +
                    attribute.getJavaType().getSimpleName() + PROP_PATCH_DELIMITER;

            if (attribute.getColumnName() == null) {
                //add ids if not present in columnsInSelect
                if (column.isExternalData())
                    for (String joinBy : column.getAttribute().joinBy()) {
                        DDataAttribute attr = classAttrubutes.stream()
                                .filter(a -> joinBy.equals(a.getColumnName()))
                                .findAny().orElse(null);
                        if (attr != null) {
                            String idKey = path + attr.getPropertyName();
                            if (!columnsInSelect.contains(idKey)) {
                                columnsInSelect.add(idKey);
                                String val = "t" + fromTableIndex + ".\"" + attr.getColumnName() + "\"";
                                sql.SELECT(val + " AS \"" + idKey + "\"");
                            }
                        }
                    }
            } else if (attribute.isMappedBean()) {
                JoinedTable jtable = allJoins.get(uniqKey);
                if (jtable == null) {
                    jtable = new JoinedTable(fromTableIndex, tablesCounter.incrementAndGet(), attribute);
                    allJoins.put(uniqKey, jtable);
                }
                final JoinedTable table = jtable;
                SelectForTableBuilder selectBuilder = new SelectForTableBuilder(
                        sql, table, columnsInSelect, alreadyJoined, pathAttributeKey, uniqKey
                );

                if (!attribute.isCollection() && column.hasFilters()) {
                    if (!alreadyJoined.contains(table.tableIndex)) {
                        sql.LEFT_OUTER_JOIN(table.joinSql);
                        alreadyJoined.add(table.tableIndex);
                    }
                    for (DDataFilter col : column.getFilters()) {
                        addColumnToViewSql(sql, attribute,
                                col, pathAttributeKey, uniqKey,
                                table.tableIndex, alreadyJoined, columnsInSelect);

                        if (col.getAttribute().isMappedBean() && !col.isExternalData()) {
                            // add non id mapping columns used by col.attribute.joinMapping.keySet
                            for (String columnName : col.getAttribute().joinBy()) {
                                selectBuilder.addNonIdAttribute(attribute, columnName);
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
                    CollectionJoin cc = selectsForCollections.get(uniqKey);
                    if (cc == null) {
                        selectsForCollections.put(uniqKey, cc = new CollectionJoin(
                                table, pathAttributeKey, uniqKey, attribute));
                    }
                    cc.filters.addAll(column.getFilters());
                }
                // add non id mapping columns used by parentAttribute.joinMapping.values
                if (parentAttribute != null)
                    for (String columnName : parentAttribute.joinOn()) {
                        selectBuilder.addNonIdAttribute(attribute, columnName);
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
        for (CollectionJoin column : selectsForCollections.values()) {
            DSQL sql = new DSQL();
            sql.FROM(rootEntity.table + " as t0");
            HashSet<Integer> joinedInSubQuery = new HashSet<>();
            HashSet<String> columnsInSubQuery = new HashSet<>();
            addJoinForSubSelects(sql, column.table, joinedInSubQuery);

            sql.SELECT(getKeySQL() + " as \"dDataBeanKey_\"");
            for (DDataFilter col : column.filters) {
                String uniqKey = column.uniqPath + col.getMapName() + ":" +
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
            for (String columnName : column.byAttribute.joinOn()) {
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
                    String key = path + filter.getMapName() + ":" +
                            innerClass.getSimpleName() + PROP_PATCH_DELIMITER;
                    addFilterSql(sql, filter, innerClass, key, table.tableIndex, alreadyJoined);
                }
            } else { // we can't use two separate filters by same table
                Class innerClass = filter.getAttribute().getJavaType();
                String key = path + filter.getMapName() + ":" +
                        innerClass.getSimpleName() + PROP_PATCH_DELIMITER;
                int finalN = tablesCounter.incrementAndGet();
                allJoins.put(key, new JoinedTable(fromTableIndex, finalN, filter.getAttribute()));
                sql.WHERE("EXISTS(" + new DSQL() {{
                    SELECT("*");
                    FROM(filter.getAttribute().joinTable() + " t" + finalN);
                    WHERE(IntStream.range(0, filter.getAttribute().joinBy().length)
                            .mapToObj(i -> "t" + fromTableIndex + "." + filter.getAttribute().joinBy()[i] +
                                    "=t" + finalN + "." + filter.getAttribute().joinOn()[i])
                            .collect(Collectors.joining(" AND ")));
                    String verSql = versionAndTypeConstraint(filter.getAttribute().getJavaType(), finalN);
                    if (verSql.length() > 0) WHERE(verSql);
                    addFilterSql(this, filter, innerClass, key, finalN, alreadyJoined);
                }}.toString() + ")");
            }
        }
    }

    private final static DateTimeFormatter sqlTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    String versionConstraint(Class clazz, int toTableIndex) {
        if (selectAllVersions()) return "";

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

    abstract boolean selectAllVersions();

    String typeConstraint(Class clazz, int toTableIndex) {
        DDataAttribute discriminant = null;
        String discriminantValue = null;
        try {
            discriminant = (DDataAttribute) clazz.getDeclaredField("DISCR_ATTR").get(null);
            discriminantValue = (String) clazz.getDeclaredField("DISCR_VAL").get(null);
        } catch (IllegalAccessException | NoSuchFieldException ignore) {
        }

        if (discriminant != null && discriminantValue != null)
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

    String getKeyType() {
        return keyType;
    }

    void addRootIdsToViewSql(DSQL sql) {
        for (DDataAttribute idAttribute : rootEntity.attributes)
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

    /**
     * Put to map reflecting object properties hierarchy.
     *
     * @param map map reflecting object properties hierarchy
     * @param key point delimited name of property
     * @param v   value
     */
    private void putInHierarchy(Map map, String key, Object v) {
        String[] p = key.split("\\.");
        int i = 0;
        Map m = map;
        for (; i < p.length - 1; i++)
            m = (Map) m.computeIfAbsent(p[i], k -> new HashMap<String, Object>());
        m.put(p[i], v);
    }


    List<Map<String, Object>> selectViewData(SqlSession sqlSession, String limitedSql) throws DDataException {
        List<Map<String, Object>> results = new ArrayList<>();
        try {
            try (PreparedStatement pst = sqlSession.getConnection().prepareStatement(limitedSql)) {
                //TODO build view from really prepared statement, what can be cached by RDBMS
                try (ResultSet rs = pst.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();
                    String[] rsColumns = new String[colCount];
                    for (int i = 0; i < colCount; ) rsColumns[i] = meta.getColumnName(++i);

                    while (rs.next()) {
                        HashMap<String, Object> row = new HashMap<>();
                        results.add(row);
                        HashSet<String> processedRemotes =
                                dictionariesService == null || remoteBeans.isEmpty() ? null : new HashSet<>();
                        for (int i = 0; i < colCount; ) {
                            String colName = rsColumns[i++];
                            TableCell column = tableCells.get(colName);
                            Object colVal;
                            if (column == null) colVal = rs.getObject(i);
                            else colVal = getFromResultSet(rs, column.attribute.getJavaType(), i);
                            putInHierarchy(row, colName, colVal);
                            if (processedRemotes != null) {
                                String path = colName.indexOf('.') > 0 ?
                                        colName.substring(0, colName.lastIndexOf('.')) :
                                        null;
                                if (!processedRemotes.contains(path)) {
                                    processedRemotes.add(path);
                                    List<RemoteBeanRef> rbl = remoteBeans.get(path);
                                    if (rbl != null)
                                        for (RemoteBeanRef remoteBeanRef : rbl) {
                                            putInHierarchy(row,
                                                    (path == null ? "" : path + ".") + remoteBeanRef.nameInPath,
                                                    getRemoteBeanValue(remoteBeanRef, rs, rsColumns));
                                        }
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("DDataView: " + limitedSql);
            LOG.error("DDataView: ", e);
            throw new DDataException("JDBC: " + e.getMessage());
        }
        return results;
    }

    private HashMap<String, Object> getRemoteBeanValue(RemoteBeanRef remoteBeanRef, ResultSet rs, String[] rsColumns) throws SQLException, DDataException {
        Object[] args = new Object[remoteBeanRef.parameters.size()];
        for (int i = 0; i < args.length; i++) {
            TableCell column = tableCells.get(remoteBeanRef.parameters.get(i));
            int j = 0;
            for (; j < rsColumns.length; )
                if (rsColumns[j++].equals(column.name)) break;
            Object colVal;
            if (column == null) colVal = rs.getObject(i);
            else colVal = getFromResultSet(rs, column.attribute.getJavaType(), j);
            args[i] = colVal;
        }
        Object o = dictionariesService.get(
                remoteBeanRef.type,
                remoteBeanRef.func,
                args);
        HashMap<String, Object> result = new HashMap<>();
        if (o != null)
            for (Method method : remoteBeanRef.type.getMethods())
                if (method.getParameterCount() == 0)
                    try {
                        String p = method.getName();
                        if (method.getName().startsWith("get")
                                && !"getDDataBeanKey_".equals(p)
                                && !"getClass".equals(p)) {
                            p = Character.toLowerCase(p.charAt(3)) + p.substring(4);
                            result.put(p, method.invoke(o));
                        } else if (method.getName().startsWith("is")) {
                            p = Character.toLowerCase(p.charAt(2)) + p.substring(3);
                            result.put(p, method.invoke(o));
                        }
                    } catch (Exception e) {
                        LOG.warn("error access to remote bean", e);
                    }
        return result;
    }

    private Object getFromResultSet(ResultSet rs, Class<?> ctype, int i) throws SQLException, DDataException {
        if (ctype.isAssignableFrom(Integer.class)) return rs.getInt(i);
        if (ctype.isAssignableFrom(Long.class)) return rs.getLong(i);
        if (ctype.isAssignableFrom(String.class)) return rs.getString(i);
        if (ctype.isAssignableFrom(LocalDateTime.class)) {
            Timestamp ts = rs.getTimestamp(i);
            return ts == null ? null : ts.toLocalDateTime();
        }
        if (ctype.isAssignableFrom(LocalDate.class)) {
            java.sql.Date ts = rs.getDate(i);
            return ts == null ? null : ts.toLocalDate();
        }
        if (ctype.isAssignableFrom(LocalTime.class)) {
            Time ts = rs.getTime(i);
            return ts == null ? null : ts.toLocalTime();
        }
        if (ctype.isAssignableFrom(Date.class) || ctype.isAssignableFrom(Timestamp.class))
            return rs.getTimestamp(i);
        if (ctype.isAssignableFrom(java.sql.Date.class)) return rs.getDate(i);
        if (ctype.isAssignableFrom(java.sql.Time.class)) return rs.getTime(i);
        if (ctype.isAssignableFrom(BigDecimal.class)) return rs.getBigDecimal(i);
        if (ctype.isAssignableFrom(BigInteger.class)) return BigInteger.valueOf(rs.getLong(i));
        if (ctype.isAssignableFrom(Boolean.class)) return rs.getBoolean(i);
        if (ctype.isAssignableFrom(Float.class)) return rs.getFloat(i);
        if (ctype.isAssignableFrom(Double.class)) return rs.getDouble(i);

        return rs.getObject(i);
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
                        IntStream.range(0, attribute.joinBy().length)
                                .mapToObj(i -> "t" + fromTableIndex + "." + attribute.joinBy()[i] +
                                        "=t" + tableIndex + "." + attribute.joinOn()[i])
                                .collect(Collectors.joining(" AND "));
                String verSql = versionAndTypeConstraint(attribute.getJavaType(), tableIndex);
                this.joinSql = joinSql + (verSql.length() > 0 ? " AND " + verSql : "") + ")";
            } else
                this.joinSql = null;
        }
    }

    private class CollectionJoin {
        private final JoinedTable table;
        private final List<DDataFilter> filters = new ArrayList<>();
        private final String path;
        private final String uniqPath;
        private final Class clazz;
        private final DDataAttribute byAttribute;

        private CollectionJoin(JoinedTable table, String path, String uniqPath, DDataAttribute byAttribute) {
            this.table = table;
            this.path = path;
            this.uniqPath = uniqPath;
            this.byAttribute = byAttribute;
            this.clazz = byAttribute.getJavaType();
        }
    }

    class TableCell {
        final String name;
        final DDataFilter column;
        final DDataAttribute attribute;
        final boolean isVersion;

        TableCell(String path, DDataFilter column, boolean isVersion) {
            this.name = path;
            this.column = column;
            this.attribute = column.getAttribute();
            this.isVersion = isVersion;
        }

        TableCell(String path, DDataAttribute attribute, boolean isVersion) {
            this.name = path;
            this.column = null;
            this.attribute = attribute;
            this.isVersion = isVersion;
        }
    }

    @SuppressWarnings("JavaReflectionMemberAccess")
    class TableEntity {
        final TableEntity parent;
        final String name;
        final Class<? extends Serializable> beanInterface;
        final List<TableCell> cells = new ArrayList<>();
        final List<TableEntity> entities = new ArrayList<>();
        final Map<TableCell, TableCell> mappings = new HashMap<>();
        final boolean collection;

        final String table;
        final DDataAttribute versionFrom;
        final DDataAttribute versionTo;
        final List<DDataAttribute> attributes;

        TableEntity(Class<? extends DDataAttribute> root) {
            this.parent = null;
            this.name = null;
            this.attributes = new ArrayList<>();
            this.collection = false;
            try {
                table = (String) root.getField("TABLE_NAME").get(null);
                versionFrom = (DDataAttribute) root.getField("VERSION_FROM").get(null);
                versionTo = (DDataAttribute) root.getField("VERSION_TO").get(null);
                beanInterface = (Class<? extends Serializable>) root.getField("BEAN_INTERFACE").get(null);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException("select view not from *_WB_ enum: " + root.getCanonicalName());
            }
        }

        TableEntity(TableEntity parent, String name, DDataFilter column) {
            this.parent = parent;
            this.name = name;
            DDataAttribute mapByAttribute = column.getAttribute();
            Class<? extends DDataAttribute> beanEnum = (Class<? extends DDataAttribute>) mapByAttribute.getJavaType();
            this.attributes = Arrays.stream(beanEnum.getEnumConstants()).collect(Collectors.toList());
            this.collection = mapByAttribute.isCollection();
            try {
                beanInterface = mapByAttribute.getBeanInterface();
                table = (String) beanEnum.getField("TABLE_NAME").get(null);
                versionFrom = (DDataAttribute) beanEnum.getField("VERSION_FROM").get(null);
                versionTo = (DDataAttribute) beanEnum.getField("VERSION_TO").get(null);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException("select view not from *_WB_ enum: " + beanEnum.getCanonicalName());
            }
        }

        boolean isCollection() {
            return collection;
        }

        void addCell(TableCell cell) {
            cells.add(cell);
            tableCells.put(cell.name, cell);
        }

        void addEntity(TableEntity entity) {
            entities.add(entity);
            tableEntities.put(entity.name, entity);
        }
    }

    class SelectForTableBuilder {
        final DSQL sql;
        final HashSet<String> columnsInSelect;
        final HashSet<Integer> alreadyJoined;
        final String pathAttributeKey;
        final String uniqKey;
        final JoinedTable table;

        SelectForTableBuilder(
                DSQL sql,
                JoinedTable table,
                HashSet<String> columnsInSelect,
                HashSet<Integer> alreadyJoined,
                String pathAttributeKey,
                String uniqKey
        ) {
            this.sql = sql;
            this.columnsInSelect = columnsInSelect;
            this.alreadyJoined = alreadyJoined;
            this.pathAttributeKey = pathAttributeKey;
            this.uniqKey = uniqKey;
            this.table = table;
        }

        void addNonIdAttribute(
                DDataAttribute attribute,
                String columnName
        ) {
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

    private class RemoteBeanRef {
        final String path;
        final String nameInPath;
        final List<String> parameters = new ArrayList<>();
        private final Class type;
        private final String func;

        RemoteBeanRef(String path, String nameInPath, Class type, String func) {
            this.path = path;
            this.nameInPath = nameInPath;
            this.type = type;
            this.func = func;
        }

        void addParameter(String parameterPath) {
            parameters.add(parameterPath);
        }
    }
}
