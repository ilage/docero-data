package org.docero.data.view;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.docero.data.utils.DDataTypes;
import org.docero.data.utils.DSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class DDataView extends AbstractDataView {
    private final static Logger LOG = LoggerFactory.getLogger(DDataView.class);

    private final SqlSession sqlSession;
    private final Class[] roots;
    private final DDataFilter[] columns;
    private DDataFilter filter = new DDataFilter();
    private final Temporal version;

    private final static Comparator<DDataAttribute> propertiesComparator = Comparator.comparing(DDataAttribute::getPropertyName);
    private final static Comparator<DDataFilter> columnsComparator = Comparator.comparing(k -> k.getAttribute().getPropertyName());
    private final HashMap<String, DDataAttribute> viewEntities = new HashMap<>();
    private final IdentityHashMap<DDataFilter, String> viewPaths = new IdentityHashMap<>();
    final List<Sort> sortedPaths = new ArrayList<>();

    private final IdentityHashMap<DDataAttribute, Set<DDataAttribute>> viewEIds = new IdentityHashMap<>();
    private final IdentityHashMap<DDataAttribute, Set<DDataFilter>> viewProperties = new IdentityHashMap<>();

    DDataView(SqlSession sqlSession, Class[] roots, DDataFilter[] columns, Temporal version) {
        this.sqlSession = sqlSession;
        this.roots = roots;
        this.columns = columns;
        if (columns.length > 0 && Arrays.stream(columns).noneMatch(c -> c.isSortAscending() != null))
            columns[0].setSortAscending(true);
        this.version = version;

        for (Field field : roots[0].getDeclaredFields())
            if (field.isEnumConstant())
                try {
                    DDataAttribute idAtr = (DDataAttribute) field.get(null);
                    if (idAtr.isPrimaryKey()) {
                        viewEIds.computeIfAbsent(null, k -> new TreeSet<>(propertiesComparator))
                                .add(idAtr);
                    }
                } catch (IllegalAccessException ignore) {
                }
        for (DDataFilter column : columns) fillViewEntities(column, null, null);
    }

    private void fillViewEntities(DDataFilter column, String path, DDataAttribute parent) {
        DDataAttribute attribute = column.getAttribute();
        if (attribute != null) {
            String nameInPath = column.mapToName() == null ? attribute.getPropertyName() : column.mapToName();
            String cp = path == null ? nameInPath : (path + "." + nameInPath);
            viewPaths.put(column, cp);
            //String[] attrPath = cp.split("\\.");
            if (attribute.isMappedBean()) {
                viewEntities.put(cp, attribute);
                for (Field field : attribute.getJavaType().getDeclaredFields())
                    if (field.isEnumConstant())
                        try {
                            DDataAttribute idAtr = (DDataAttribute) field.get(null);
                            if (idAtr.isPrimaryKey()) {
                                viewEIds.computeIfAbsent(attribute, k -> new TreeSet<>(propertiesComparator))
                                        .add(idAtr);
                            }
                        } catch (IllegalAccessException ignore) {
                        }
            } else if (!attribute.isPrimaryKey() && attribute.getColumnName() != null) {
                if (column.isSortAscending() != null)
                    sortedPaths.add(new Sort(cp, column.isSortAscending()));

                viewProperties.computeIfAbsent(parent, k -> new TreeSet<>(columnsComparator)).add(column);
            }

            if (column.getFilters() != null) column.getFilters()
                    .forEach(f -> fillViewEntities(f, cp, attribute));
        } else
            if (column.getFilters() != null) column.getFilters()
                .forEach(f -> fillViewEntities(f, path, parent));
    }

    Temporal version() {
        return version;
    }

    public DDataFilter getFilter() {
        return filter;
    }

    public void setFilter(DDataFilter filter) {
        this.filter = filter;
    }

    public long count() throws DDataException {
        DSQL sql = buildFrom(roots[0]);
        sql.SELECT("COUNT(*)");
        buildFilters(sql);

        return sqlSession.selectOne("org.docero.data.selectCount",
                Collections.singletonMap("sqlStatement", sql.toString()));
    }

    public DDataViewRows select(int offset, int limit) throws DDataException {
        this.updates = new HashMap<>();
        DSQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column);
                    break;
                }
        super.addRootIdsToViewSql(sql);
        buildFilters(sql);
        String limitedSql = addBounds(sql.toString(), offset, limit);

        //if(LOG.isDebugEnabled()) LOG.debug("Preparing: "+sql.toString());
        Map<Object, Object> resultMap = sqlSession.selectMap(
                "org.docero.data.selectView",
                Collections.singletonMap("sqlStatement", limitedSql), "dDataBeanKey_");
        //if(LOG.isDebugEnabled()) LOG.debug("Total: "+resultMap.size());
        if (resultMap.size() > 0) {
            String in_condition = keySql + " IN (" + resultMap.keySet().stream()
                    .map(k -> DDataTypes.maskedValue(getKeyType(), k.toString()))
                    .collect(Collectors.joining(",")) +
                    ")";
            for (DSQL subSelect : getSubSelects()) {
                subSelect.WHERE(in_condition);
                //if(LOG.isDebugEnabled()) LOG.debug("Preparing: "+subSelect.toString());
                List<Map<Object, Object>> subResult = sqlSession.selectList(
                        "org.docero.data.selectView",
                        Collections.singletonMap("sqlStatement", subSelect.toString()));
                //if(LOG.isDebugEnabled()) LOG.debug("Total: "+subResult.size());
                for (Map<Object, Object> row : subResult) {
                    Object key = row.get("dDataBeanKey_");
                    mergeResultMaps(key, resultMap, row);
                }
            }
        }
        return new DDataViewRows(this, resultMap);
    }

    @SuppressWarnings("unchecked")
    public int[] aggregateInt(DDataFilterOperator operator) throws DDataException {
        DSQL agSql = new DSQL();
        agSql.SELECT("'group' as \"dDataBeanKey_\"");
        DSQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column);
                    String pathName = column.mapToName() != null ? column.mapToName() : column.getName();
                    agSql.SELECT(operator + "(t.\"" + pathName + "\") AS \"" + pathName + "\"");
                    break;
                }
        buildFilters(sql);
        sql.GROUP_BY(keySql);
        agSql.FROM("(" + sql.toString() + ") AS t");
        Map<Object, Object> result = sqlSession.selectMap(
                "org.docero.data.selectView",
                Collections.singletonMap("sqlStatement", agSql.toString()), "dDataBeanKey_");
        Map<Object, Object> row = (Map<Object, Object>) result.get("group");
        if (row == null) return new int[0];
        int[] ret = new int[row.size() - 1];
        for (int i = 0; i < columns.length; i++) {
            Object ro = row.get(columns[i].mapToName() != null ? columns[i].mapToName() : columns[i].getName());
            if (ro instanceof Number) ret[i] = ((Number) ro).intValue();
        }
        return ret;
    }

    private String addBounds(String s, int offset, int limit) {
        String limits = (offset > 0 ? " OFFSET " + offset : "") +
                (limit > 0 ? " LIMIT " + limit : "");
        if (s.endsWith(";")) {
            return s.substring(0, s.length() - 1) + limits + ";";
        } else {
            return s + limits;
        }
    }

    @SuppressWarnings("unchecked")
    private void mergeResultMaps(Object to_key, Map<Object, Object> to, Object leaf) {
        if (leaf == null || to_key == null) return;
        if (leaf instanceof Map && ((Map) leaf).containsKey("!")) {
            leaf = ((Map) leaf).get("!");
        }
        final Object finalLeaf = leaf;
        Object val = to.get(to_key);
        if (val == null) {
            to.put(to_key, new ArrayList<Object>() {{
                this.add(finalLeaf);
            }});
        } else if (val instanceof List) {
            ((List) val).add(finalLeaf);
        } else if (val instanceof Map && finalLeaf instanceof Map) {
            for (Object vk : ((Map) finalLeaf).keySet())
                if (!"dDataBeanKey_".equals(vk)) {
                    mergeResultMaps(vk, (Map<Object, Object>) val, ((Map) finalLeaf).get(vk));
                }
        } else {
            to.put(to_key, new ArrayList<Object>() {{
                this.add(val);
                this.add(finalLeaf);
            }});
        }
    }

    private void buildFilters(DSQL sql) throws DDataException {
        DDataFilter allTypesFilter = new DDataFilter();
        allTypesFilter.getFilters().addAll(
                filter.getFilters().stream().filter(f ->
                        Arrays.stream(roots).allMatch(r -> isApplicable(r, f))
                ).collect(Collectors.toList())
        );
        DDataFilter someTypesFilter = new DDataFilter();
        someTypesFilter.getFilters().addAll(
                filter.getFilters().stream().filter(
                        f -> !allTypesFilter.getFilters().contains(f)
                ).collect(Collectors.toList())
        );

        super.addFilterSql(sql, allTypesFilter, roots[0]);
        String vc = versionConstraint(roots[0], 0);
        if (vc.length() > 0) sql.WHERE(vc);

        DSQL ssql = new DSQL();
        for (int i = 0; i < roots.length; i++) {
            if (i > 0) ssql.OR();

            Class multiTypeClass = roots[i];
            String tc = typeConstraint(multiTypeClass, 0);
            if (tc.length() > 0) ssql.WHERE(tc);

            super.addFilterSql(ssql, someTypesFilter, multiTypeClass);
        }
        sql.WHERE(ssql);
    }

    /**
     * row -> beanPath -> index -> parameter path
     */
    private HashMap<DDataViewRow, HashMap<String, Set<Integer>>> updates;

    DDataAttribute getAttributeForPath(String s) {
        return viewEntities.get(s);
    }

    void addUpdate(DDataViewRow dDataViewRow, int index, String path) {
        int i = path.lastIndexOf('.');
        String beanPath = i < 0 ? null : path.substring(0, i);
        HashMap<String, Set<Integer>> update =
                updates.computeIfAbsent(dDataViewRow, k -> new HashMap<>());
        update.computeIfAbsent(beanPath, k -> new HashSet<>()).add(index);
    }

    private class PreparedUpdates {
        final String path;
        final DDataAttribute beanAttribute;
        final Set<DDataAttribute> ids;
        final Set<DDataFilter> props;
        final Set<DDataAttribute> unModified;
        final Class beanClass;
        final String tableName;
        final DDataAttribute versionFrom;
        final DDataAttribute versionTo;

        PreparedStatement ps;
        boolean batchOperation;

        PreparedUpdates(String entityPropertyPath) throws DDataException {
            this.path = entityPropertyPath;
            beanAttribute = viewEntities.get(entityPropertyPath);
            ids = viewEIds.get(beanAttribute);
            props = viewProperties.get(beanAttribute);

            beanClass = beanAttribute != null ? beanAttribute.getJavaType() : roots[0];
            unModified = new TreeSet<>(propertiesComparator);
                for (Field field : beanClass.getDeclaredFields())
                    if (field.isEnumConstant())
                        try {
                            DDataAttribute beanAtr = (DDataAttribute) field.get(null);
                            if (!beanAtr.isPrimaryKey() && !beanAtr.isMappedBean() && beanAtr.getColumnName() != null) {
                                if (props == null || props.stream().noneMatch(c -> c.getAttribute() == beanAtr))
                                    unModified.add(beanAtr);
                            }
                        } catch (IllegalAccessException ignore) {
                        }
            try {
                tableName = (String) beanClass.getField("TABLE_NAME").get(null);
                versionFrom = (DDataAttribute) beanClass.getField("VERSION_FROM").get(null);
                versionTo = (DDataAttribute) beanClass.getField("VERSION_TO").get(null);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new DDataException("incorrect enumeration used for filter");
            }
            if (props == null || ids == null || tableName == null)
                throw new DDataException("props==null || ids==null || tableName==null");
        }
    }

    public void flushUpdates() throws SQLException, DDataException {
        if (updates == null || updates.size() == 0) return;
        Date dateNow = new Date();

        Connection connection = sqlSession.getConnection();
        Set<PreparedUpdates> prepared = new TreeSet<>(Comparator.comparingInt(k -> k.path.length()));
        try {
            for (DDataViewRow row : updates.keySet()) {
                for (String entityPropertyPath : updates.get(row).keySet()) {
                    PreparedUpdates pk = prepared.stream()
                            .filter(k -> entityPropertyPath.equals(k.path))
                            .findAny().orElse(null);

                    if (pk == null)
                        prepared.add(pk = buildStatement(connection, entityPropertyPath));

                    for (Integer updatedIndex : updates.get(row).get(entityPropertyPath)) {
                        Object anyId = row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, pk.ids.iterator().next().getPropertyName()));

                        if (anyId == null) { //is new element

                            //TODO or not TODO

                        } else if (pk.versionFrom != null) { //versional bean
                            int pIdx = 1;
                            if (pk.versionTo != null) {
                                setColumnValue(pk.ps, pIdx++, pk.versionTo, dateNow);
                                for (DDataAttribute id : pk.ids)
                                    setColumnValue(pk.ps, pIdx++, id,
                                            row.getColumnValue(updatedIndex,
                                                    expandPath(entityPropertyPath, id.getPropertyName())));
                            }
                            for (DDataFilter prop : pk.props)
                                setColumnValue(pk.ps, pIdx++, prop.getAttribute(),
                                        row.getColumnValue(updatedIndex, viewPaths.get(prop)));

                            setColumnValue(pk.ps, pIdx++, pk.versionFrom, dateNow);

                            for (DDataAttribute id : pk.ids)
                                setColumnValue(pk.ps, pIdx++, id,
                                        row.getColumnValue(updatedIndex,
                                                expandPath(entityPropertyPath, id.getPropertyName())));
                            if (LOG.isTraceEnabled()) {
                                LOG.trace(pk.ps.toString());
                                LOG.trace("Parameters: " + (pk.versionTo != null ? (
                                        dateNow.toString() + "," +
                                                pk.ids.stream().map(id ->
                                                        row.getColumnValue(updatedIndex,
                                                                expandPath(entityPropertyPath, id.getPropertyName()))
                                                                .toString()
                                                ).collect(Collectors.joining(",")) + ",") :
                                        "") + pk.props.stream()
                                        .map(prop -> row.getColumnValue(updatedIndex, viewPaths.get(prop)))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(",")) +
                                        "," + dateNow.toString() + "," +
                                        pk.ids.stream().map(id ->
                                                row.getColumnValue(updatedIndex,
                                                        expandPath(entityPropertyPath, id.getPropertyName()))
                                                        .toString()
                                        ).collect(Collectors.joining(","))
                                );
                            }
                            pk.ps.executeUpdate();
                        } else {
                            int pIdx = 1;
                            for (DDataFilter prop : pk.props)
                                setColumnValue(pk.ps, pIdx++, prop.getAttribute(),
                                        row.getColumnValue(updatedIndex, viewPaths.get(prop)));
                            for (DDataAttribute id : pk.ids)
                                setColumnValue(pk.ps, pIdx++, id,
                                        row.getColumnValue(updatedIndex,
                                                expandPath(entityPropertyPath, id.getPropertyName())));
                            if (LOG.isTraceEnabled()) {
                                LOG.trace(pk.ps.toString());
                                LOG.trace("Parameters: " +
                                        pk.props.stream().map(prop -> row.getColumnValue(updatedIndex, viewPaths.get(prop)).toString())
                                                .collect(Collectors.joining(",")) + "," +
                                        pk.ids.stream().map(id ->
                                                row.getColumnValue(updatedIndex, expandPath(entityPropertyPath, id.getPropertyName())).toString()
                                        ).collect(Collectors.joining(","))
                                );
                            }
                            pk.ps.addBatch();
                        }
                    }
                }
            }
            for (PreparedUpdates pk : prepared) if (pk.batchOperation) pk.ps.execute();
        } finally {
            for (PreparedUpdates pk : prepared) pk.ps.close();
        }
    }

    private PreparedUpdates buildStatement(Connection connection, String entityPropertyPath) throws SQLException, DDataException {
        PreparedUpdates pk = new PreparedUpdates(entityPropertyPath);
        if (pk.versionFrom != null) {
            String valuesExceptModified = pk.unModified.isEmpty() ? "" :
                    pk.unModified.stream()
                            .map(DDataAttribute::getColumnName)
                            .collect(Collectors.joining(",")) + ",";
            String sql = (pk.versionTo != null ?
                    "UPDATE " + pk.tableName + " SET " + pk.versionTo.getColumnName() +
                            "=? WHERE " + pk.ids.stream().map(p -> p.getColumnName() + "=?")
                            .collect(Collectors.joining(" AND ")) + "; " :
                    "") +
                    "INSERT INTO " + pk.tableName + " (" +
                    //all ids except version
                    pk.ids.stream().filter(a -> a != pk.versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    pk.props.stream()
                            .map(c -> c.getAttribute().getColumnName())
                            .collect(Collectors.joining(",")) +
                    "," + pk.versionFrom.getColumnName() +
                    ") SELECT " +
                    //all ids except version
                    pk.ids.stream().filter(a -> a != pk.versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    pk.props.stream()
                            .map(p -> "?")
                            .collect(Collectors.joining(",")) +
                    ",?" +//value of version
                    " FROM " + pk.tableName +
                    " WHERE " + pk.ids.stream().map(p -> p.getColumnName() + "=?")
                    .collect(Collectors.joining(" AND "));
            pk.ps = connection.prepareStatement(sql);
            pk.batchOperation = false;
            return pk;
        } else {
            String sql = "UPDATE " + pk.tableName +
                    " SET " + pk.props.stream()
                    .map(p -> p.getAttribute().getColumnName() + "=?")
                    .collect(Collectors.joining(",")) +
                    " WHERE " + pk.ids.stream().map(p -> p.getColumnName() + "=?")
                    .collect(Collectors.joining(" AND "));
            pk.ps = connection.prepareStatement(sql);
            pk.batchOperation = true;
            return pk;
        }
    }

    private String expandPath(String path, String propertyName) {
        return path.length() == 0 ? propertyName : path + "." + propertyName;
    }

    private void setColumnValue(
            PreparedStatement ps, int pIdx, DDataAttribute prop, Object v
    ) throws SQLException {
        if ("TIMESTAMP".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.TIMESTAMP);
            else if (v instanceof Timestamp)
                ps.setTimestamp(pIdx, (Timestamp) v);
            else if (v instanceof LocalDateTime)
                ps.setTimestamp(pIdx, Timestamp.valueOf((LocalDateTime) v));
            else if (v instanceof Date)
                ps.setTimestamp(pIdx, new Timestamp(((Date) v).getTime()));
            else
                ps.setNull(pIdx, Types.TIMESTAMP);
        } else if ("DATE".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.DATE);
            else if (v instanceof java.sql.Date)
                ps.setDate(pIdx, (java.sql.Date) v);
            else if (v instanceof LocalDate)
                ps.setDate(pIdx, java.sql.Date.valueOf((LocalDate) v));
            else if (v instanceof Date)
                ps.setDate(pIdx, new java.sql.Date(((Date) v).getTime()));
            else
                ps.setNull(pIdx, Types.DATE);
        } else if ("TIME".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.TIME);
            else if (v instanceof java.sql.Time)
                ps.setTime(pIdx, (java.sql.Time) v);
            else if (v instanceof Date)
                ps.setTime(pIdx, new java.sql.Time(((Date) v).getTime()));
            else if (v instanceof LocalTime)
                ps.setTime(pIdx, java.sql.Time.valueOf((LocalTime) v));
            else
                ps.setNull(pIdx, Types.TIME);
        } else if ("VARCHAR".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.VARCHAR);
            else if (v instanceof String)
                ps.setString(pIdx, v.toString());
        } else if ("BOOLEAN".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.BOOLEAN);
            else if (v instanceof Boolean)
                ps.setBoolean(pIdx, (Boolean) v);
            else
                ps.setNull(pIdx, Types.BOOLEAN);
        } else if ("SMALLINT".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.SMALLINT);
            else if (v instanceof Number)
                ps.setShort(pIdx, ((Number) v).shortValue());
            else
                ps.setNull(pIdx, Types.SMALLINT);
        } else if ("INTEGER".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.INTEGER);
            else if (v instanceof Number)
                ps.setInt(pIdx, ((Number) v).intValue());
            else
                ps.setNull(pIdx, Types.INTEGER);
        } else if ("BIGINT".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.BIGINT);
            else if (v instanceof Number)
                ps.setLong(pIdx, ((Number) v).longValue());
            else
                ps.setNull(pIdx, Types.BIGINT);
        } else if ("REAL".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.REAL);
            else if (v instanceof Number)
                ps.setFloat(pIdx, ((Number) v).floatValue());
            else
                ps.setNull(pIdx, Types.REAL);
        } else if ("DOUBLE".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.DOUBLE);
            else if (v instanceof Number)
                ps.setDouble(pIdx, ((Number) v).doubleValue());
            else
                ps.setNull(pIdx, Types.DOUBLE);
        } else if ("NUMERIC".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.NUMERIC);
            else if (v instanceof BigDecimal)
                ps.setBigDecimal(pIdx, (BigDecimal) v);
            else if (v instanceof Number)
                ps.setBigDecimal(pIdx, BigDecimal.valueOf(((Number) v).doubleValue()));
            else
                ps.setNull(pIdx, Types.NUMERIC);
        } else if ("BINARY".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.BINARY);
            else if (v instanceof byte[])
                ps.setBytes(pIdx, (byte[]) v);
            else if (v instanceof InputStream)
                ps.setBinaryStream(pIdx, (InputStream) v);
            else
                ps.setNull(pIdx, Types.BINARY);
        }
    }

    static class Sort {
        final String path;
        final boolean asc;

        Sort(String path, boolean asc) {
            this.path = path;
            this.asc = asc;
        }
    }
}
