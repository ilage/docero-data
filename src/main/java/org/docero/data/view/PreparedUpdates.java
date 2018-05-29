package org.docero.data.view;

import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
class PreparedUpdates {
    private final static Logger LOG = LoggerFactory.getLogger(PreparedUpdates.class);

    final String entityPropertyPath;

    private final DDataView dDataView;
    private final TreeSet<DDataAttribute> ids = new TreeSet<>(DDataView.propertiesComparator);
    private final TreeSet<DDataFilter> props = new TreeSet<>(DDataView.columnsComparator);
    private final DDataAttribute versionFrom;
    private final DDataAttribute versionTo;

    private final Connection connection;
    private final TreeSet<DDataAttribute> unModified;
    private final Map<DDataAttribute, MapPath> mappings;
    private String tableName;
    private PreparedStatement ps = null;
    private PreparedStatement pi = null;
    private boolean batchOperation = false;

    PreparedUpdates(DDataView dDataView, String entityPropertyPath, Connection connection) throws DDataException, SQLException {
        this.entityPropertyPath = entityPropertyPath;
        this.dDataView = dDataView;
        this.connection = connection;
        DDataAttribute beanAttribute = dDataView.getEntityForPath(entityPropertyPath);
        ids.addAll(dDataView.viewEIds.get(beanAttribute));
        Set<DDataFilter> beanProperties = dDataView.viewProperties.get(beanAttribute);
        props.addAll(beanProperties);
        mappings = new HashMap<>();
        dDataView.viewMappings.values().forEach(e -> e.stream()
                .filter(m -> entityPropertyPath == null ?
                        m.parentPath.indexOf('.') < 0 :
                        m.parentPath.startsWith(entityPropertyPath + ".") &&
                                m.parentPath.indexOf('.', entityPropertyPath.length() + 1) < 0)
                .filter(m -> !m.parentAttribute.isPrimaryKey())
                .forEach(m -> mappings.put(m.parentAttribute, new MapPath(m.childPath, false))));
        dDataView.viewMappings.values().forEach(e -> e.stream()
                .filter(m -> entityPropertyPath == null ?
                        m.childPath.indexOf('.') < 0 :
                        m.childPath.startsWith(entityPropertyPath + ".") &&
                                m.childPath.indexOf('.', entityPropertyPath.length() + 1) < 0)
                .filter(m -> !m.childAttribute.isPrimaryKey())
                .forEach(m -> mappings.put(m.childAttribute, new MapPath(m.parentPath,
                        beanAttribute != null && beanAttribute.isCollection()))));

        Class<? extends DDataAttribute> beanClass = beanAttribute != null ?
                beanAttribute.getJavaType() : dDataView.roots[0];
        unModified = new TreeSet<>(DDataView.propertiesComparator);
        for (DDataAttribute beanAtr : beanAttribute != null ? Arrays.asList(beanClass.getEnumConstants()) : dDataView.rootAttributes)
            if (!beanAtr.isPrimaryKey() && !beanAtr.isMappedBean() && beanAtr.getColumnName() != null) {
                if (props.stream().noneMatch(c -> c.getAttribute() == beanAtr) &&
                        !mappings.containsKey(beanAtr))
                    unModified.add(beanAtr);
            }
        try {
            tableName = (String) beanClass.getField("TABLE_NAME").get(null);
            versionFrom = (DDataAttribute) beanClass.getField("VERSION_FROM").get(null);
            versionTo = (DDataAttribute) beanClass.getField("VERSION_TO").get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new DDataException("incorrect enumeration used for filter");
        }
        if (props.isEmpty() || ids.isEmpty() || tableName == null)
            throw new DDataException("props==null || ids==null || tableName==null");
    }

    void fillUpdate(DDataViewRow row, Integer updatedIndex, java.util.Date dateNow) throws SQLException {
        if (ps == null) prepareUpdate();

        if (versionFrom != null) { //versional bean
            int pIdx = 1;
            if (versionTo != null) {
                fillStatement(ps, pIdx++, versionTo, dateNow);
                for (DDataAttribute id : ids)
                    fillStatement(ps, pIdx++, id,
                            row.getColumnValue(updatedIndex,
                                    expandPath(entityPropertyPath, id.getPropertyName())));
                fillStatement(ps, pIdx++, versionFrom,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, versionFrom.getPropertyName())));
            }

            for (DDataFilter prop : props)
                fillStatement(ps, pIdx++, prop.getAttribute(),
                        row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)));

            for (Map.Entry<DDataAttribute, MapPath> m : mappings.entrySet()) {
                fillStatement(ps, pIdx++, m.getKey(),
                        row.getColumnValue(m.getValue().fromCollection ? 0 : updatedIndex, m.getValue().path));
            }

            fillStatement(ps, pIdx++, versionFrom, dateNow);

            for (DDataAttribute id : ids)
                fillStatement(ps, pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, id.getPropertyName())));
            fillStatement(ps, pIdx++, versionFrom,
                    row.getColumnValue(updatedIndex,
                            expandPath(entityPropertyPath, versionFrom.getPropertyName())));

            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " + (versionTo != null ? (
                                "NOW()," +
                                        ids.stream().map(id ->
                                                row.getColumnValue(updatedIndex,
                                                        expandPath(entityPropertyPath, id.getPropertyName()))
                                                        .toString()
                                        ).collect(Collectors.joining(",")) + "," +
                                        row.getColumnValue(updatedIndex,
                                                expandPath(entityPropertyPath, versionFrom.getPropertyName())) +
                                        ","
                        ) :
                                "") +
                                props.stream()
                                        .map(prop -> row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(",")) +
                                (mappings.isEmpty() ? "" : "," +
                                        mappings.values().stream()
                                                .map(m -> row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.path))
                                                .map(val -> val == null ? "NULL" : val.toString())
                                                .collect(Collectors.joining(","))
                                ) +
                                ",NOW()," +
                                ids.stream().map(id ->
                                        row.getColumnValue(updatedIndex,
                                                expandPath(entityPropertyPath, id.getPropertyName())))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(",")) + "," +
                                row.getColumnValue(updatedIndex,
                                        expandPath(entityPropertyPath, versionFrom.getPropertyName()))
                );
            }
            ps.executeUpdate();
        } else {
            int pIdx = 1;
            for (DDataFilter prop : props)
                fillStatement(ps, pIdx++, prop.getAttribute(),
                        row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)));
            for (Map.Entry<DDataAttribute, MapPath> m : mappings.entrySet()) {
                fillStatement(ps, pIdx++, m.getKey(),
                        row.getColumnValue(m.getValue().fromCollection ? 0 : updatedIndex, m.getValue().path));
            }
            for (DDataAttribute id : ids)
                fillStatement(ps, pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, id.getPropertyName())));
            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " +
                        props.stream().map(prop -> row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)))
                                .map(val -> val == null ? "NULL" : val.toString())
                                .collect(Collectors.joining(",")) +
                        (mappings.isEmpty() ? "" : "," +
                                mappings.values().stream()
                                        .map(m -> row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.path))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(","))
                        ) + "," +
                        ids.stream().map(id ->
                                row.getColumnValue(updatedIndex, expandPath(entityPropertyPath, id.getPropertyName())))
                                .map(val -> val == null ? "NULL" : val.toString())
                                .collect(Collectors.joining(","))
                );
            }
            ps.addBatch();
        }
    }

    /**
     * Prepare statement
     */
    private void prepareUpdate() throws SQLException {
        if (versionFrom != null) {
            String valuesExceptModified = unModified.stream()
                    .filter(c -> versionTo == null || !versionTo.getColumnName().equals(c.getColumnName()))
                    .map(DDataAttribute::getColumnName)
                    .map(s -> "\"" + s + "\"")
                    .collect(Collectors.joining(","));
            if (valuesExceptModified.length() > 0) valuesExceptModified += ",";

            String sql = (versionTo != null ?
                    "UPDATE " + tableName + " SET \"" + versionTo.getColumnName() +
                            "\"=? WHERE " + ids.stream().map(p -> "\"" + p.getColumnName() + "\"=?")
                            .collect(Collectors.joining(" AND ")) + " AND \"" +
                            versionFrom.getColumnName() + "\"=?; " :
                    "") +
                    "INSERT INTO " + tableName + " (" +
                    //all ids except version
                    ids.stream().filter(a -> a != versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .map(s -> "\"" + s + "\"")
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    props.stream()
                            .map(c -> c.getAttribute().getColumnName())
                            .map(s -> "\"" + s + "\"")
                            .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," +
                            mappings.entrySet().stream()
                                    .map(m -> m.getKey().getColumnName())
                                    .map(s -> "\"" + s + "\"")
                                    .collect(Collectors.joining(","))) +
                    ",\"" + versionFrom.getColumnName() +
                    "\") SELECT " +
                    //all ids except version
                    ids.stream().filter(a -> a != versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .map(s -> "\"" + s + "\"")
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    parameters(props.size() + mappings.size()) +
                    ",?" +//value of version
                    " FROM " + tableName +
                    " WHERE " + ids.stream().map(p -> "\"" + p.getColumnName() + "\"=?")
                    .collect(Collectors.joining(" AND ")) +
                    " AND \"" + versionFrom.getColumnName() + "\"=?";
            ps = connection.prepareStatement(sql);
            batchOperation = false;
        } else {
            String sql = "UPDATE " + tableName +
                    " SET " + props.stream()
                    .map(p -> "\"" + p.getAttribute().getColumnName() + "\"=?")
                    .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," + mappings.entrySet().stream()
                            .map(m -> "\"" + m.getKey().getColumnName() + "\"=?")
                            .collect(Collectors.joining(","))) +
                    " WHERE " + ids.stream().map(p -> "\"" + p.getColumnName() + "\"=?")
                    .collect(Collectors.joining(" AND "));
            ps = connection.prepareStatement(sql);
            batchOperation = true;
        }
    }

    private String expandPath(String path, String propertyName) {
        return path == null || path.length() == 0 ? propertyName : path + "." + propertyName;
    }

    private void fillStatement(PreparedStatement ps, int pIdx, DDataAttribute prop, Object v) throws SQLException {
        if ("TIMESTAMP".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.TIMESTAMP);
            else if (v instanceof Timestamp)
                ps.setTimestamp(pIdx, (Timestamp) v);
            else if (v instanceof LocalDateTime)
                ps.setTimestamp(pIdx, Timestamp.valueOf((LocalDateTime) v));
            else if (v instanceof java.util.Date)
                ps.setTimestamp(pIdx, new Timestamp(((java.util.Date) v).getTime()));
            else
                ps.setNull(pIdx, Types.TIMESTAMP);
        } else if ("DATE".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.DATE);
            else if (v instanceof java.sql.Date)
                ps.setDate(pIdx, (java.sql.Date) v);
            else if (v instanceof LocalDate)
                ps.setDate(pIdx, java.sql.Date.valueOf((LocalDate) v));
            else if (v instanceof java.util.Date)
                ps.setDate(pIdx, new java.sql.Date(((java.util.Date) v).getTime()));
            else
                ps.setNull(pIdx, Types.DATE);
        } else if ("TIME".equals(prop.getJdbcType())) {
            if (v == null)
                ps.setNull(pIdx, Types.TIME);
            else if (v instanceof Time)
                ps.setTime(pIdx, (Time) v);
            else if (v instanceof java.util.Date)
                ps.setTime(pIdx, new Time(((java.util.Date) v).getTime()));
            else if (v instanceof LocalTime)
                ps.setTime(pIdx, Time.valueOf((LocalTime) v));
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

    String getFirstIdColumnName() {
        return ids.iterator().next().getPropertyName();
    }

    void fillInsert(DDataViewRow row, Integer updatedIndex, java.util.Date dateNow) throws SQLException {
        if (pi == null) prepareInsert();

        int pIdx = 1;
        for (DDataFilter prop : props)
            fillStatement(pi, pIdx++, prop.getAttribute(),
                    row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)));
        for (Map.Entry<DDataAttribute, MapPath> m : mappings.entrySet()) {
            fillStatement(pi, pIdx++, m.getKey(),
                    row.getColumnValue(m.getValue().fromCollection ? 0 : updatedIndex, m.getValue().path));
        }

        for (DDataAttribute id : ids)
            fillStatement(pi, pIdx++, id,
                    row.getColumnValue(updatedIndex,
                            expandPath(entityPropertyPath, id.getPropertyName())));
        if (versionFrom != null)
            fillStatement(pi, pIdx++, versionFrom, dateNow);

        if (LOG.isTraceEnabled()) {
            LOG.trace(pi.toString());
            LOG.trace("Parameters: " +
                    props.stream().map(prop -> row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)))
                            .map(val -> val == null ? "NULL" : val.toString())
                            .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," +
                            mappings.values().stream()
                                    .map(m -> row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.path))
                                    .map(val -> val == null ? "NULL" : val.toString())
                                    .collect(Collectors.joining(","))
                    ) + "," +
                    ids.stream().map(id ->
                            row.getColumnValue(updatedIndex, expandPath(entityPropertyPath, id.getPropertyName())))
                            .map(val -> val == null ? "NULL" : val.toString())
                            .collect(Collectors.joining(",")) +
                    (versionFrom == null ? "" : ",NOW()")
            );
        }
        pi.executeUpdate();
    }

    private void prepareInsert() throws SQLException {
        String sql = "INSERT INTO " + tableName + " (" +
                props.stream()
                        .map(p -> p.getAttribute().getColumnName())
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",")) +
                (mappings.isEmpty() ? "" : "," +
                        mappings.entrySet().stream()
                                .map(m -> m.getKey().getColumnName())
                                .map(s -> "\"" + s + "\"")
                                .collect(Collectors.joining(","))
                ) + "," +
                ids.stream().map(DDataAttribute::getColumnName)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",")) +
                (versionFrom == null ? "" : ",\"" + versionFrom.getColumnName() + "\"") +
                ") VALUES (" + parameters(props.size() + mappings.size() +
                ids.size() + (versionFrom == null ? 0 : 1)) + ")";
        pi = connection.prepareStatement(sql);
    }

    private String parameters(int n) {
        StringBuilder sb = new StringBuilder("?");
        for (int i = 1; i < n; i++) sb.append(",?");
        return sb.toString();
    }

    void execute() throws SQLException {
        if (batchOperation && ps != null) ps.execute();
    }

    void close() {
        if (ps != null) try {
            ps.close();
        } catch (Exception ignore) {
        }
        if (pi != null) try {
            pi.close();
        } catch (Exception ignore) {
        }
    }

    /**
     * Fill mapping values between parent and child beans.
     *
     * @param row                 - data row
     * @param entityBeanAttribute - attribute used for mapping (mappedBean=true)
     * @param updatedIndex        - index of array value (parent entity)
     * @param entityPropertyPath  - path to attribute (child entity)
     */
    void fillMappings(DDataViewRow row, DDataAttribute entityBeanAttribute, Integer updatedIndex, String entityPropertyPath) {
        for (Map.Entry<String, String> m : entityBeanAttribute.joinMapping().entrySet()) {
            @SuppressWarnings("unchecked")
            Class<? extends DDataAttribute> beanClass = entityBeanAttribute.getClass();
            DDataAttribute beanMapAttribute = Arrays.stream(beanClass.getEnumConstants())
                    .filter(a -> m.getKey().equals(a.getColumnName()))
                    .findAny().orElse(null);
            @SuppressWarnings("unchecked")
            Class<? extends DDataAttribute> childClass = (Class<? extends DDataAttribute>) entityBeanAttribute.getJavaType();
            DDataAttribute childMapAttribute = Arrays.stream(childClass.getEnumConstants())
                    .filter(a -> m.getValue().equals(a.getColumnName()))
                    .findAny().orElse(null);
            if (beanMapAttribute != null && childMapAttribute != null) {
                int lii = entityPropertyPath.lastIndexOf('.');
                String parentPropertyPrefix = lii < 0 ? "" :
                        entityPropertyPath.substring(0, lii + 1);
                int parentIndex = entityBeanAttribute.isCollection() ? 0 : updatedIndex;
                if (!beanMapAttribute.isPrimaryKey()) {
                    row.setColumnValue(
                            row.getColumnValue(updatedIndex, entityPropertyPath + "." +
                                    childMapAttribute.getPropertyName()),
                            parentIndex, parentPropertyPrefix +
                                    beanMapAttribute.getPropertyName(),
                            false);
                    if (LOG.isDebugEnabled())
                        LOG.debug("fill parent property '" + parentPropertyPrefix + beanMapAttribute.getPropertyName() +
                                "' from child entity property '" + entityPropertyPath + "." + childMapAttribute.getPropertyName() +
                                "' with value: " + row.getColumnValue(updatedIndex, entityPropertyPath + "." +
                                childMapAttribute.getPropertyName()));
                } else if (!childMapAttribute.isPrimaryKey()) {
                    row.setColumnValue(
                            row.getColumnValue(parentIndex, parentPropertyPrefix +
                                    beanMapAttribute.getPropertyName()),
                            updatedIndex, entityPropertyPath + "." +
                                    childMapAttribute.getPropertyName(),
                            false);
                    if (LOG.isDebugEnabled())
                        LOG.debug("fill child entity property '" + entityPropertyPath + "." + childMapAttribute.getPropertyName() +
                                "' from parent property '" + parentPropertyPrefix + beanMapAttribute.getPropertyName() +
                                "' with value: " + row.getColumnValue(parentIndex, parentPropertyPrefix +
                                beanMapAttribute.getPropertyName()));
                } else if (LOG.isDebugEnabled())
                    LOG.debug("both properties are primaryKeys '" + m.getKey() + "' (" +
                            parentPropertyPrefix + beanMapAttribute.getPropertyName() +
                            ") -> '" + m.getValue() + "' (" +
                            entityPropertyPath + "." + childMapAttribute.getPropertyName() +
                            ")");
            } else if (LOG.isDebugEnabled())
                LOG.debug("no mapping found for " + m.getKey() + " (" +
                        (beanMapAttribute == null ? "NULL" : beanMapAttribute.getPropertyName()) +
                        ") ->" + m.getValue() + " (" +
                        (childMapAttribute == null ? "NULL" : childMapAttribute.getPropertyName()) +
                        ")");
        }
    }

    private class MapPath {
        final String path;
        final boolean fromCollection;

        private MapPath(String path, boolean fromCollection) {
            this.path = path;
            this.fromCollection = fromCollection;
        }
    }
}
