package org.docero.data.view;

import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
class PreparedUpdates implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(PreparedUpdates.class);

    final AbstractDataView.TableEntity entity;

    /**
     * ids without version
     */
    private final List<DDataAttribute> ids = new ArrayList<>();
    private final List<AbstractDataView.TableCell> props = new ArrayList<>();

    private final Connection connection;
    private final Map<String, PreparedMap> mappings;
    private final List<DDataAttribute> unModified;
    private final List<PreparedStatement> psl = new ArrayList<>();

    PreparedUpdates(DDataView dDataView, AbstractDataView.TableEntity entity, Connection connection) throws DDataException, SQLException {
        this.connection = connection;
        this.entity = entity;
        ids.addAll(entity.cells.stream()
                .filter(c -> !c.isVersion && c.attribute.isPrimaryKey())
                .map(c -> c.attribute).collect(Collectors.toSet())
        );
        mappings = new HashMap<>();
        entity.mappings.entrySet().stream()
                .filter(m -> !m.getKey().attribute.isPrimaryKey() && !m.getKey().isVersion)
                .flatMap(m -> m.getValue().values().stream()
                        .flatMap(v -> Collections.singletonMap(m.getKey(), v).entrySet().stream())
                )
                .filter(m -> dDataView.tableCells.values().contains(m.getValue()))
                .forEach(m -> mappings.put(m.getKey().name, new PreparedMap(m.getKey(), m.getValue(), false)));
        if (entity.parent != null)
            entity.parent.mappings.entrySet().stream()
                    .filter(m -> m.getValue().get(entity) != null &&
                            !m.getValue().get(entity).attribute.isPrimaryKey() && !m.getValue().get(entity).isVersion)
                    .filter(m -> dDataView.tableCells.values().contains(m.getKey()))
                    .forEach(m -> {
                        AbstractDataView.TableCell cell = m.getValue().get(entity);
                        mappings.put(cell.name, new PreparedMap(cell, m.getKey(), entity.isCollection()));
                    });
        props.addAll(entity.cells.stream()
                .filter(c -> c.column != null && !c.isVersion && !c.attribute.isPrimaryKey()
                        && mappings.values().stream().noneMatch(m -> DDataAttribute.equals(m.to.attribute, c.attribute))
                        && entity.fixedColumns.stream().noneMatch(fc -> DDataAttribute.equals(fc.getAttribute(), c.attribute))
                ).collect(Collectors.toSet())
        );

        unModified = new ArrayList<>();
        for (DDataAttribute beanAtr : entity.attributes)
            if (!beanAtr.isPrimaryKey() && !beanAtr.isMappedBean() && beanAtr.getColumnName() != null) {
                if (props.stream().noneMatch(c -> DDataAttribute.equals(c.attribute, beanAtr)) &&
                        mappings.values().stream().noneMatch(m -> DDataAttribute.equals(m.to.attribute, beanAtr)))
                    unModified.add(beanAtr);
            }
        if (props.isEmpty() || ids.isEmpty())
            throw new DDataException("props==null || ids==null");
    }

    void fillUpdate(DDataViewRow row, Integer updatedIndex, java.util.Date dateNow) throws SQLException {
        PreparedStatement ps = prepareUpdate();
        if (entity.versionFrom != null) { //versional bean
            int pIdx = 1;
            if (entity.versionTo != null) {
                //UPDATE some SET versionTo=? WHERE id...=? AND version=?;
                fillStatement(ps, pIdx++, entity.versionTo, dateNow);
                for (DDataAttribute id : ids)
                    fillStatement(ps, pIdx++, id,
                            row.getColumnValue(updatedIndex,
                                    propertyPath(id.getPropertyName())));
                fillStatement(ps, pIdx++, entity.versionFrom,
                        row.getColumnValue(updatedIndex,
                                propertyPath(entity.versionFrom.getPropertyName())));
            }
            //INSERT INTO some (<columns>) SELECT <columns without updated>, ?..., version
            for (AbstractDataView.TableCell prop : props)
                fillStatement(ps, pIdx++, prop.attribute,
                        row.getColumnValue(updatedIndex, prop.name));
            for (PreparedMap m : mappings.values()) {
                fillStatement(ps, pIdx++, m.to.attribute,
                        row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.from.name));
            }
            fillStatement(ps, pIdx++, entity.versionFrom, dateNow);
            //FROM some WHERE id...=? AND version=?
            for (DDataAttribute id : ids)
                fillStatement(ps, pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                propertyPath(id.getPropertyName())));
            fillStatement(ps, pIdx, entity.versionFrom,
                    row.getColumnValue(updatedIndex,
                            propertyPath(entity.versionFrom.getPropertyName())));

            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " + (entity.versionTo != null ? (
                                "NOW(?)," +
                                        ids.stream().map(id ->
                                                row.getColumnValue(updatedIndex,
                                                        propertyPath(id.getPropertyName()))
                                                        .toString()
                                        ).collect(Collectors.joining(",")) + "," +
                                        row.getColumnValue(updatedIndex,
                                                propertyPath(entity.versionFrom.getPropertyName())) +
                                        ","
                        ) :
                                "") +
                                props.stream()
                                        .map(prop -> row.getColumnValue(updatedIndex, prop.name))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(",")) +
                                (mappings.isEmpty() ? "" : "," +
                                        mappings.values().stream()
                                                .map(m -> row.getColumnValue(
                                                        m.fromCollection ? 0 : updatedIndex, m.from.name))
                                                .map(val -> val == null ? "NULL" : val.toString())
                                                .collect(Collectors.joining(","))
                                ) +
                                ",NOW(?)," +
                                ids.stream().map(id ->
                                        row.getColumnValue(updatedIndex,
                                                propertyPath(id.getPropertyName())))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(",")) + "," +
                                row.getColumnValue(updatedIndex,
                                        propertyPath(entity.versionFrom.getPropertyName()))
                );
            }
        } else {
            int pIdx = 1;
            for (AbstractDataView.TableCell prop : props)
                fillStatement(ps, pIdx++, prop.attribute,
                        row.getColumnValue(updatedIndex, prop.name));
            for (PreparedMap m : mappings.values()) {
                fillStatement(ps, pIdx++, m.to.attribute,
                        row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.from.name));
            }
            for (DDataAttribute id : ids)
                fillStatement(ps, pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                propertyPath(id.getPropertyName())));
            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " +
                        props.stream().map(prop -> row.getColumnValue(updatedIndex, prop.name))
                                .map(val -> val == null ? "NULL" : val.toString())
                                .collect(Collectors.joining(",")) +
                        (mappings.isEmpty() ? "" : "," +
                                mappings.values().stream()
                                        .map(m -> row.getColumnValue(
                                                m.fromCollection ? 0 : updatedIndex, m.from.name))
                                        .map(val -> val == null ? "NULL" : val.toString())
                                        .collect(Collectors.joining(","))
                        ) + "," +
                        ids.stream().map(id ->
                                row.getColumnValue(updatedIndex, propertyPath(id.getPropertyName())))
                                .map(val -> val == null ? "NULL" : val.toString())
                                .collect(Collectors.joining(","))
                );
            }
        }
        psl.add(ps);
    }

    /**
     * Prepare statement
     */
    private PreparedStatement prepareUpdate() throws SQLException {
        PreparedStatement ps;
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
                            mappings.values().stream()
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
        } else {
            String sql = "UPDATE " + entity.table +
                    " SET " + props.stream()
                    .map(p -> "\"" + p.attribute.getColumnName() + "\"=?")
                    .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," + mappings.values().stream()
                            .map(m -> "\"" + m.to.attribute.getColumnName() + "\"=?")
                            .collect(Collectors.joining(","))) +
                    " WHERE " + ids.stream().map(p -> "\"" + p.getColumnName() + "\"=?")
                    .collect(Collectors.joining(" AND "));
            ps = connection.prepareStatement(sql);
        }
        return ps;
    }

    private String propertyPath(String propertyName) {
        return entity.name == null || entity.name.length() == 0 ?
                propertyName :
                entity.name + "." + propertyName;
    }

    private void fillStatement(PreparedStatement ps, int pIdx, DDataAttribute prop, Object v) throws SQLException {
        if (!prop.isNullable() && v == null)
            throw new SQLException("Column '" + prop.getColumnName() + "' don't allows NULL");

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
        PreparedStatement pi = prepareInsert();

        int pIdx = 1;
        for (AbstractDataView.TableCell prop : props)
            fillStatement(pi, pIdx++, prop.attribute,
                    row.getColumnValue(updatedIndex, prop.name));
        for (PreparedMap m : mappings.values()) {
            fillStatement(pi, pIdx++, m.to.attribute,
                    row.getColumnValue(m.fromCollection ? 0 : updatedIndex, m.from.name));
        }
        for (DDataFilter fixedColumn : entity.fixedColumns) {
            fillStatement(pi, pIdx++, fixedColumn.getAttribute(), fixedColumn.getValue());
        }

        for (DDataAttribute id : ids)
            fillStatement(pi, pIdx++, id,
                    row.getColumnValue(updatedIndex,
                            propertyPath(id.getPropertyName())));
        if (entity.versionFrom != null)
            fillStatement(pi, pIdx, entity.versionFrom, dateNow);

        if (LOG.isTraceEnabled()) {
            LOG.trace(pi.toString());
            LOG.trace("Parameters: " +
                    props.stream().map(prop -> row.getColumnValue(updatedIndex, prop.name))
                            .map(val -> val == null ? "NULL" : val.toString())
                            .collect(Collectors.joining(",")) +
                    (mappings.isEmpty() ? "" : "," +
                            mappings.values().stream()
                                    .map(m -> row.getColumnValue(
                                            m.fromCollection ? 0 : updatedIndex, m.from.name))
                                    .map(val -> val == null ? "NULL" : val.toString())
                                    .collect(Collectors.joining(","))
                    ) +
                    (entity.fixedColumns.isEmpty() ? "" : "," +
                            entity.fixedColumns.stream()
                                    .map(DDataFilter::getValue)
                                    .map(val -> val == null ? "NULL" : val.toString())
                                    .collect(Collectors.joining(","))
                    ) + "," +
                    ids.stream().map(id ->
                            row.getColumnValue(updatedIndex, propertyPath(id.getPropertyName())))
                            .map(val -> val == null ? "NULL" : val.toString())
                            .collect(Collectors.joining(",")) +
                    (entity.versionFrom == null ? "" : ",NOW(?)")
            );
        }
        psl.add(pi);
    }

    private PreparedStatement prepareInsert() throws SQLException {
        String sql = "INSERT INTO " + entity.table + " (" +
                props.stream()
                        .map(p -> p.attribute.getColumnName())
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",")) +
                (mappings.isEmpty() ? "" : "," +
                        mappings.values().stream()
                                .map(m -> m.to.attribute.getColumnName())
                                .map(s -> "\"" + s + "\"")
                                .collect(Collectors.joining(","))
                ) +
                (entity.fixedColumns.isEmpty() ? "" : "," +
                        entity.fixedColumns.stream()
                                .map(m -> m.getAttribute().getColumnName())
                                .map(s -> "\"" + s + "\"")
                                .collect(Collectors.joining(","))
                ) + "," +
                ids.stream().map(DDataAttribute::getColumnName)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",")) +
                (entity.versionFrom == null ? "" : ",\"" + entity.versionFrom.getColumnName() + "\"") +
                ") VALUES (" + parameters(props.size() + mappings.size() + entity.fixedColumns.size() +
                ids.size() + (entity.versionFrom == null ? 0 : 1)) + ")";
        return connection.prepareStatement(sql);
    }

    private String parameters(int n) {
        StringBuilder sb = new StringBuilder("?");
        for (int i = 1; i < n; i++) sb.append(",?");
        return sb.toString();
    }

    void execute() throws SQLException {
        for (PreparedStatement ps : psl) ps.execute();
    }

    public void close() {
        if (!psl.isEmpty())
            for (PreparedStatement ps : psl)
                try {
                    ps.close();
                } catch (Exception ignore) {
                }
    }

    /**
     * Fill mapping non-null values between parent and child beans.
     *
     * @param row          - data row
     * @param updatedIndex - index of array value (parent entity)
     */
    void fillMappings(DDataViewRow row, Integer updatedIndex) {
        for (Map.Entry<AbstractDataView.TableCell, Map<AbstractDataView.TableEntity, AbstractDataView.TableCell>> m :
                entity.mappings.entrySet()) {
            DDataAttribute beanMapAttribute = m.getKey().attribute;
            for (AbstractDataView.TableCell childMapCell : m.getValue().values()) {
                DDataAttribute childMapAttribute = childMapCell.attribute;
                int lii = entity.name.lastIndexOf('.');
                String parentMappedProperty = (lii < 0 ? "" : entity.name.substring(0, lii + 1))
                        + beanMapAttribute.getPropertyName();
                String entityMappedProperty = entity.name + "." + childMapAttribute.getPropertyName();
                int parentIndex = entity.isCollection() ? updatedIndex : 0;
                try {
                    if (!beanMapAttribute.isPrimaryKey()) {
                        Object v = row.getColumnValue(updatedIndex, entityMappedProperty);
                        if (v != null) row.setColumnValue(v, parentIndex, parentMappedProperty, false);
                        if (LOG.isDebugEnabled())
                            LOG.debug("fill parent property '" + parentMappedProperty +
                                    "' from child entity property '" + entityMappedProperty +
                                    "' with value: " + row.getColumnValue(updatedIndex, entityMappedProperty));
                    } else if (!childMapAttribute.isPrimaryKey()) {
                        Object v = row.getColumnValue(parentIndex, parentMappedProperty);
                        if (v != null) row.setColumnValue(v,
                                updatedIndex, entityMappedProperty,
                                false);
                        if (LOG.isDebugEnabled())
                            LOG.debug("fill child entity property '" + entityMappedProperty +
                                    "' from parent property '" + parentMappedProperty +
                                    "' with value: " + row.getColumnValue(parentIndex, parentMappedProperty));
                    } else if (LOG.isDebugEnabled())
                        LOG.debug("both properties are primaryKeys '" + m.getKey() + "' (" +
                                parentMappedProperty + ") -> '" + m.getValue() + "' (" + entityMappedProperty + ")");
                } catch (RuntimeException ignore) {
                    LOG.debug("may be correct: not available " + entityMappedProperty);
                }
            }
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
