package org.docero.data.view;

import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
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

    PreparedStatement ps;
    boolean batchOperation;

    PreparedUpdates(DDataView dDataView, String entityPropertyPath, Connection connection) throws DDataException, SQLException {
        this.entityPropertyPath = entityPropertyPath;
        this.dDataView = dDataView;
        DDataAttribute beanAttribute = dDataView.getEntityForPath(entityPropertyPath);
        ids.addAll(dDataView.viewEIds.get(beanAttribute));
        Set<DDataFilter> beanProperties = dDataView.viewProperties.get(beanAttribute);
        props.addAll(beanProperties);

        Class beanClass = beanAttribute != null ? beanAttribute.getJavaType() : dDataView.roots[0];
        Set<DDataAttribute> unModified = new TreeSet<>(DDataView.propertiesComparator);
        for (Field field : beanClass.getDeclaredFields())
            if (field.isEnumConstant())
                try {
                    DDataAttribute beanAtr = (DDataAttribute) field.get(null);
                    if (!beanAtr.isPrimaryKey() && !beanAtr.isMappedBean() && beanAtr.getColumnName() != null) {
                        if (props.stream().noneMatch(c -> c.getAttribute() == beanAtr))
                            unModified.add(beanAtr);
                    }
                } catch (IllegalAccessException ignore) {
                }
        String tableName;
        try {
            tableName = (String) beanClass.getField("TABLE_NAME").get(null);
            versionFrom = (DDataAttribute) beanClass.getField("VERSION_FROM").get(null);
            versionTo = (DDataAttribute) beanClass.getField("VERSION_TO").get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new DDataException("incorrect enumeration used for filter");
        }
        if (props.isEmpty() || ids.isEmpty() || tableName == null)
            throw new DDataException("props==null || ids==null || tableName==null");

        /*
            Prepare statement
         */
        if (versionFrom != null) {
            String valuesExceptModified = unModified.isEmpty() ? "" :
                    unModified.stream()
                            .filter(c -> versionTo == null || !versionTo.getColumnName().equals(c.getColumnName()))
                            .map(DDataAttribute::getColumnName)
                            .collect(Collectors.joining(",")) + ",";
            String sql = (versionTo != null ?
                    "UPDATE " + tableName + " SET " + versionTo.getColumnName() +
                            "=? WHERE " + ids.stream().map(p -> p.getColumnName() + "=?")
                            .collect(Collectors.joining(" AND ")) + "; " :
                    "") +
                    "INSERT INTO " + tableName + " (" +
                    //all ids except version
                    ids.stream().filter(a -> a != versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    props.stream()
                            .map(c -> c.getAttribute().getColumnName())
                            .collect(Collectors.joining(",")) +
                    "," + versionFrom.getColumnName() +
                    ") SELECT " +
                    //all ids except version
                    ids.stream().filter(a -> a != versionFrom)
                            .map(DDataAttribute::getColumnName)
                            .collect(Collectors.joining(",")) +
                    "," +
                    //all values except modified by view
                    valuesExceptModified +
                    //values modified by view
                    props.stream()
                            .map(p -> "?")
                            .collect(Collectors.joining(",")) +
                    ",?" +//value of version
                    " FROM " + tableName +
                    " WHERE " + ids.stream().map(p -> p.getColumnName() + "=?")
                    .collect(Collectors.joining(" AND "));
            ps = connection.prepareStatement(sql);
            batchOperation = false;
        } else {
            String sql = "UPDATE " + tableName +
                    " SET " + props.stream()
                    .map(p -> p.getAttribute().getColumnName() + "=?")
                    .collect(Collectors.joining(",")) +
                    " WHERE " + ids.stream().map(p -> p.getColumnName() + "=?")
                    .collect(Collectors.joining(" AND "));
            ps = connection.prepareStatement(sql);
            batchOperation = true;
        }
    }

    void fillStatement(DDataViewRow row, Integer updatedIndex, Date dateNow) throws SQLException {
        if (versionFrom != null) { //versional bean
            int pIdx = 1;
            if (versionTo != null) {
                fillStatement(pIdx++, versionTo, dateNow);
                for (DDataAttribute id : ids)
                    fillStatement(pIdx++, id,
                            row.getColumnValue(updatedIndex,
                                    expandPath(entityPropertyPath, id.getPropertyName())));
            }
            for (DDataFilter prop : props)
                fillStatement(pIdx++, prop.getAttribute(),
                        row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)));

            fillStatement(pIdx++, versionFrom, dateNow);

            for (DDataAttribute id : ids)
                fillStatement(pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, id.getPropertyName())));
            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " + (versionTo != null ? (
                        dateNow.toString() + "," +
                                ids.stream().map(id ->
                                        row.getColumnValue(updatedIndex,
                                                expandPath(entityPropertyPath, id.getPropertyName()))
                                                .toString()
                                ).collect(Collectors.joining(",")) + ",") :
                        "") + props.stream()
                        .map(prop -> row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)))
                        .map(val -> val == null ? "NULL" : val.toString())
                        .collect(Collectors.joining(",")) +
                        "," + dateNow.toString() + "," +
                        ids.stream().map(id ->
                                row.getColumnValue(updatedIndex,
                                        expandPath(entityPropertyPath, id.getPropertyName()))
                                        .toString()
                        ).collect(Collectors.joining(","))
                );
            }
            ps.executeUpdate();
        } else {
            int pIdx = 1;
            for (DDataFilter prop : props)
                fillStatement(pIdx++, prop.getAttribute(),
                        row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)));
            for (DDataAttribute id : ids)
                fillStatement(pIdx++, id,
                        row.getColumnValue(updatedIndex,
                                expandPath(entityPropertyPath, id.getPropertyName())));
            if (LOG.isTraceEnabled()) {
                LOG.trace(ps.toString());
                LOG.trace("Parameters: " +
                        props.stream().map(prop -> row.getColumnValue(updatedIndex, dDataView.getPathForColumn(prop)).toString())
                                .collect(Collectors.joining(",")) + "," +
                        ids.stream().map(id ->
                                row.getColumnValue(updatedIndex, expandPath(entityPropertyPath, id.getPropertyName())).toString()
                        ).collect(Collectors.joining(","))
                );
            }
            ps.addBatch();
        }
    }

    private String expandPath(String path, String propertyName) {
        return path.length() == 0 ? propertyName : path + "." + propertyName;
    }

    private void fillStatement(int pIdx, DDataAttribute prop, Object v) throws SQLException {
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
            else if (v instanceof Time)
                ps.setTime(pIdx, (Time) v);
            else if (v instanceof Date)
                ps.setTime(pIdx, new Time(((Date) v).getTime()));
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
}
