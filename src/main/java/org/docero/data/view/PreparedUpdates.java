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
    private final List<DDataAttribute> ids = new ArrayList<>();
    private final List<AbstractDataView.TableCell> props = new ArrayList<>();
    private final AbstractDataView.TableEntity entity;

    private final Connection connection;
    private final List<PreparedMap> mappings;
    private final List<DDataAttribute> unModified;
    private PreparedStatement ps = null;
    private PreparedStatement pi = null;
    private boolean batchOperation = false;

    PreparedUpdates(DDataView dDataView, String entityPropertyPath, Connection connection) throws DDataException, SQLException {
        this.entityPropertyPath = entityPropertyPath;
        this.dDataView = dDataView;
        this.connection = connection;
        this.entity = dDataView.getEntityForPath(entityPropertyPath);
        ids.addAll(entity.cells.stream()
                .filter(c -> !c.isVersion && c.attribute.isPrimaryKey())
                .map(c -> c.attribute).collect(Collectors.toSet())
        );
        mappings = new ArrayList<>();
        entity.mappings.entrySet().stream()
                .filter(m -> !m.getKey().attribute.isPrimaryKey() && !m.getKey().isVersion)
                .forEach(m -> mappings.add(new PreparedMap(m.getKey(), m.getValue(), false)));
        if (entity.parent != null)
            entity.parent.mappings.entrySet().stream()
                    .filter(m -> entity.cells.contains(m.getValue()) && !m.getValue().attribute.isPrimaryKey() && !m.getValue().isVersion)
                    .forEach(m -> mappings.add(new PreparedMap(m.getValue(), m.getKey(), entity.isCollection())));

        props.addAll(entity.cells.stream()
                .filter(c -> c.column != null && !c.isVersion && !c.attribute.isPrimaryKey() &&
                        mappings.stream().noneMatch(m -> m.to.attribute == c.attribute))
                .collect(Collectors.toSet())
        );

        unModified = new ArrayList<>();
        for (DDataAttribute beanAtr : entity.attributes)
            if (!beanAtr.isPrimaryKey() && !beanAtr.isMappedBean() && beanAtr.getColumnName() != null) {
                if (props.stream().noneMatch(c -> c.attribute == beanAtr) &&
                        mappings.stream().noneMatch(m -> m.to.attribute == beanAtr))
                    unModified.add(beanAtr);
            }
        if (props.isEmpty() || ids.isEmpty())
            throw new DDataException("props==null || ids==null");
    }

    void fillUpdate(DDataViewRow row, Integer updatedIndex, java.util.Date dateNow) throws SQLException {
        if (ps == null) prepareUpdate();

        if (entity.versionFrom != null) { //versional bean
            int pIdx = 1;
            if (entity.versionTo != null) {
                fillStatement(ps, pIdx++, entity.versionTo, dateNow);
                for (DDataAttribute id : ids)
                    fillStatement(ps, pIdx++, id,
                            row.getColumnValue(updatedIndex,
                                    expandPath(entityPropertyPath, id.getPropertyName())));
                fillStatement(ps, pIdx++, entity.versionFrom,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, entity.versionFrom.getPropertyName())));
            }

            for (AbstractDataView.TableCell prop : props)
                fillStatement(ps, pIdx++, prop.attribute,
                        row.getColumnValue(updatedIndex, prop.name));

            for (PreparedMap m : mappings) {
                fillStatement(ps, pIdx++, m.to.attribute,
                        row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.from.name));
            }

            fillStatement(ps, pIdx++, entity.versionFrom, dateNow);

            for (DDataAttribute id : ids)
                fillStatement(ps, pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, id.getPropertyName())));
            fillStatement(ps, pIdx++, entity.versionFrom,
                    row.getColumnValue(updatedIndex,
                            expandPath(entityPropertyPath, entity.versionFrom.getPropertyName())));

            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " + (entity.versionTo != null ? (
                                "NOW()," +
                                        ids.stream().map(id ->
                                                row.getColumnValue(updatedIndex,
                                                        expandPath(entityPropertyPath, id.getPropertyName()))
                                                        .toString()
                                        ).collect(Collectors.joining(",")) + "," +
                                        row.getColumnValue(updatedIndex,
                                                expandPath(entityPropertyPath, entity.versionFrom.getPropertyName())) +
                                        ","
                        ) :
                                "") +
                                props.stream()
                                        .map(prop -> row.getColumnValue(updatedIndex, prop.name))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(",")) +
                                (mappings.isEmpty() ? "" : "," +
                                        mappings.stream()
                                                .map(m -> row.getColumnValue(
                                                        m.fromCollection ? 0 : updatedIndex, m.from.name))
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
                                        expandPath(entityPropertyPath, entity.versionFrom.getPropertyName()))
                );
            }
            ps.executeUpdate();
        } else {
            int pIdx = 1;
            for (AbstractDataView.TableCell prop : props)
                fillStatement(ps, pIdx++, prop.attribute,
                        row.getColumnValue(updatedIndex, prop.name));
            for (PreparedMap m : mappings) {
                fillStatement(ps, pIdx++, m.to.attribute,
                        row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.from.name));
            }
            for (DDataAttribute id : ids)
                fillStatement(ps, pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, id.getPropertyName())));
            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " +
                        props.stream().map(prop -> row.getColumnValue(updatedIndex, prop.name))
                                .map(val -> val == null ? "NULL" : val.toString())
                                .collect(Collectors.joining(",")) +
                        (mappings.isEmpty() ? "" : "," +
                                mappings.stream()
                                        .map(m -> row.getColumnValue(
                                                m.fromCollection ? 0 : updatedIndex, m.from.name))
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
        if (entity.versionFrom != null) {
            String valuesExceptModified = unModified.stream()
                    .filter(c -> entity.versionTo == null || !entity.versionTo.getColumnName().equals(c.getColumnName()))
                    .map(DDataAttribute::getColumnName)
                    .map(s -> "\"" + s + "\"")
                    .collect(Collectors.joining(","));
            if (valuesExceptModified.length() > 0) valuesExceptModified += ",";

            String sql = (entity.versionTo != null ?
                    "UPDATE " + entity.table + " SET \"" + entity.versionTo.getColumnName() +
                            "\"=? WHERE " + ids.stream().map(p -> "\"" + p.getColumnName() + "\"=?")
                            .collect(Collectors.joining(" AND ")) + " AND \"" +
                            entity.versionFrom.getColumnName() + "\"=?; " :
                    "") +
                    "INSERT INTO " + entity.table + " (" +
                    //all ids except version
                    ids.stream().filter(a -> a != entity.versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .map(s -> "\"" + s + "\"")
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    props.stream()
                            .map(c -> c.attribute.getColumnName())
                            .map(s -> "\"" + s + "\"")
                            .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," +
                            mappings.stream()
                                    .map(m -> m.to.attribute.getColumnName())
                                    .map(s -> "\"" + s + "\"")
                                    .collect(Collectors.joining(","))) +
                    ",\"" + entity.versionFrom.getColumnName() +
                    "\") SELECT " +
                    //all ids except version
                    ids.stream().filter(a -> a != entity.versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .map(s -> "\"" + s + "\"")
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    parameters(props.size() + mappings.size()) +
                    ",?" +//value of version
                    " FROM " + entity.table +
                    " WHERE " + ids.stream().map(p -> "\"" + p.getColumnName() + "\"=?")
                    .collect(Collectors.joining(" AND ")) +
                    " AND \"" + entity.versionFrom.getColumnName() + "\"=?";
            ps = connection.prepareStatement(sql);
            batchOperation = false;
        } else {
            String sql = "UPDATE " + entity.table +
                    " SET " + props.stream()
                    .map(p -> "\"" + p.attribute.getColumnName() + "\"=?")
                    .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," + mappings.stream()
                            .map(m -> "\"" + m.to.attribute.getColumnName() + "\"=?")
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
        for (AbstractDataView.TableCell prop : props)
            fillStatement(pi, pIdx++, prop.attribute,
                    row.getColumnValue(updatedIndex, prop.name));
        for (PreparedMap m : mappings) {
            fillStatement(pi, pIdx++, m.to.attribute,
                    row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.from.name));
        }

        for (DDataAttribute id : ids)
            fillStatement(pi, pIdx++, id,
                    row.getColumnValue(updatedIndex,
                            expandPath(entityPropertyPath, id.getPropertyName())));
        if (entity.versionFrom != null)
            fillStatement(pi, pIdx++, entity.versionFrom, dateNow);

        if (LOG.isTraceEnabled()) {
            LOG.trace(pi.toString());
            LOG.trace("Parameters: " +
                    props.stream().map(prop -> row.getColumnValue(updatedIndex, prop.name))
                            .map(val -> val == null ? "NULL" : val.toString())
                            .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," +
                            mappings.stream()
                                    .map(m -> row.getColumnValue(
                                            m.fromCollection ? 0 : updatedIndex, m.from.name))
                                    .map(val -> val == null ? "NULL" : val.toString())
                                    .collect(Collectors.joining(","))
                    ) + "," +
                    ids.stream().map(id ->
                            row.getColumnValue(updatedIndex, expandPath(entityPropertyPath, id.getPropertyName())))
                            .map(val -> val == null ? "NULL" : val.toString())
                            .collect(Collectors.joining(",")) +
                    (entity.versionFrom == null ? "" : ",NOW()")
            );
        }
        pi.executeUpdate();
    }

    private void prepareInsert() throws SQLException {
        String sql = "INSERT INTO " + entity.table + " (" +
                props.stream()
                        .map(p -> p.attribute.getColumnName())
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",")) +
                (mappings.isEmpty() ? "" : "," +
                        mappings.stream()
                                .map(m -> m.to.attribute.getColumnName())
                                .map(s -> "\"" + s + "\"")
                                .collect(Collectors.joining(","))
                ) + "," +
                ids.stream().map(DDataAttribute::getColumnName)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",")) +
                (entity.versionFrom == null ? "" : ",\"" + entity.versionFrom.getColumnName() + "\"") +
                ") VALUES (" + parameters(props.size() + mappings.size() +
                ids.size() + (entity.versionFrom == null ? 0 : 1)) + ")";
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
     * @param row                - data row
     * @param entity             - entity (parent entity)
     * @param updatedIndex       - index of array value (parent entity)
     * @param entityPropertyPath - path to attribute (child entity)
     */
    void fillMappings(DDataViewRow row, AbstractDataView.TableEntity entity, Integer updatedIndex, String entityPropertyPath) {
        for (Map.Entry<AbstractDataView.TableCell, AbstractDataView.TableCell> m : entity.mappings.entrySet()) {
            DDataAttribute beanMapAttribute = m.getKey().attribute;
            DDataAttribute childMapAttribute = m.getValue().attribute;
            int lii = entityPropertyPath.lastIndexOf('.');
            String parentPropertyPrefix = lii < 0 ? "" :
                    entityPropertyPath.substring(0, lii + 1);
            int parentIndex = entity.isCollection() ? 0 : updatedIndex;
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
        }
    }

    private class PreparedMap {
        final AbstractDataView.TableCell to;
        final AbstractDataView.TableCell from;
        final boolean fromCollection;

        private PreparedMap(AbstractDataView.TableCell to, AbstractDataView.TableCell from, boolean fromCollection) {
            this.to = to;
            this.from = from;
            this.fromCollection = fromCollection;
        }
    }
}
