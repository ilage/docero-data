package org.docero.data.view;

import org.apache.ibatis.jdbc.SQL;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataTypes;

import java.lang.reflect.Field;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
abstract class AbstractDataView {
    private static final String PROP_PATCH_DELIMITER = ".";
    private final AtomicInteger tablesCounter = new AtomicInteger(0);

    abstract Temporal version();

    void addColumnToViewSql(
            SQL sql, HashMap<String, Integer> usedCols, Class clazz,
            DDataFilter column, String path, String uniqPath, int fromTableIndex
    ) {
        DDataAttribute attribute = null;
        for (Field field : clazz.getDeclaredFields())
            if (field.isEnumConstant()) {
                DDataAttribute a = (DDataAttribute) Enum.valueOf(clazz, field.getName());
                if (a.getPropertyName().equals(column.getName())) {
                    attribute = a;
                    break;
                }
            }

        if (attribute != null && attribute.getColumnName() != null) {
            String pathAttributeName = path + attribute.getPropertyName();
            String pathAttributeKey = pathAttributeName + PROP_PATCH_DELIMITER;
            String uniqKey = uniqPath + attribute.getPropertyName() + ":" +
                    attribute.getJavaType().getSimpleName() + PROP_PATCH_DELIMITER;

            if (attribute.isMappedBean()) {
                Integer tableIndex = usedCols.get(uniqKey);
                if (tableIndex == null) {
                    tableIndex = tablesCounter.incrementAndGet();
                    int finalN = tableIndex;
                    String joinSql = attribute.joinTable() + " t" + finalN + " ON (" +
                            attribute.joinMapping().entrySet().stream()
                                    .map(m -> "t" + fromTableIndex + "." + m.getKey() +
                                            "=t" + finalN + "." + m.getValue())
                                    .collect(Collectors.joining(" AND "));
                    String verSql = versionAndTypeConstraint(attribute.getJavaType(), finalN);
                    sql.LEFT_OUTER_JOIN(joinSql + (verSql.length() > 0 ? " AND " + verSql : "") + ")");
                    usedCols.put(uniqKey, tableIndex);
                }

                if (attribute.getJavaType().isEnum() && !attribute.isCollection() && column.hasFilters()) {

                    for (DDataFilter col : column.getFilters()) {
                        addColumnToViewSql(sql, usedCols, attribute.getJavaType(), col,
                                pathAttributeKey, uniqKey, tableIndex);
                    }

                }
            } else {
                sql.SELECT("t" + fromTableIndex + "." + attribute.getColumnName() + " AS \"" + pathAttributeName + "\"");
            }
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

    void addFilterSql(SQL sql, DDataFilter rootFilter, Class rootClass, Map<String, Integer> usedCols, String path, final int fromTableIndex) {
        if (rootFilter == null) return;

        Integer tableIndex = usedCols.get(path);
        if (tableIndex == null) {
            tableIndex = tablesCounter.incrementAndGet();
            final int finalN = tableIndex;
            String joinSql = rootFilter.getAttribute().joinTable() + " t" + tableIndex + " ON (" +
                    rootFilter.getAttribute().joinMapping().entrySet().stream()
                            .map(m -> "t" + fromTableIndex + "." + m.getKey() +
                                    "=t" + finalN + "." + m.getValue())
                            .collect(Collectors.joining(" AND "));
            String verSql = versionAndTypeConstraint(rootFilter.getAttribute().getJavaType(), tableIndex);
            sql.LEFT_OUTER_JOIN(joinSql + (verSql.length() > 0 ? " AND " + verSql : "") + ")");
            usedCols.put(path, tableIndex);
        }

        List<DDataFilter> appliedFilters = rootFilter.getFilters() == null ? Collections.emptyList() :
                rootFilter.getFilters().stream()
                        .filter(f -> isApplicable(rootClass, f))
                        .collect(Collectors.toList());

        for (DDataFilter filter : appliedFilters) {
            if (!filter.getAttribute().isCollection()) {
                if (filter.getOperator() != null && !filter.hasFilters()) {
                    String columnReference = "t" + tableIndex + ".\"" +
                            filter.getAttribute().getColumnName() + "\" ";
                    String columnType = filter.getAttribute().getJdbcType();
                    String condition;
                    switch (filter.getOperator().getOperands()) {
                        case 0:
                            condition = columnReference + filter.getOperator().toString();
                            break;
                        case 1:
                            String value = filter.getValue().toString();
                            if (filter.getOperator() == DDataFilterOperator.LIKE) value = "%" + value + "%";
                            else if (filter.getOperator() == DDataFilterOperator.NOT_LIKE) value = "%" + value + "%";
                            else if (filter.getOperator() == DDataFilterOperator.STARTS) value = value + "%";
                            else if (filter.getOperator() == DDataFilterOperator.NOT_STARTS) value = value + "%";
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
                    addFilterSql(sql, filter, innerClass, usedCols, key, tableIndex);
                }
            } else { // we can't use two separate filters by same table
                Class innerClass = filter.getAttribute().getJavaType();
                String key = path + filter.getAttribute().getPropertyName() + ":" +
                        innerClass.getSimpleName() + PROP_PATCH_DELIMITER;
                int finalN = tablesCounter.incrementAndGet();
                usedCols.put(key, finalN);
                sql.WHERE("EXISTS(" + new SQL() {{
                    SELECT("*");
                    FROM(filter.getAttribute().joinTable() + " t" + finalN);
                    WHERE(filter.getAttribute().joinMapping().entrySet().stream()
                            .map(m -> "t" + fromTableIndex + "." + m.getKey() + "=t" + finalN + "." + m.getValue())
                            .collect(Collectors.joining(" AND ")));
                    String verSql = versionAndTypeConstraint(filter.getAttribute().getJavaType(), finalN);
                    if(verSql.length()>0) WHERE(verSql);
                    addFilterSql(this, filter, innerClass, usedCols, key, finalN);
                }}.toString() + ")");
            }
        }
    }

    private final static DateTimeFormatter sqlTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    String versionAndTypeConstraint(Class clazz, int toTableIndex) {
        DDataAttribute versionFrom = null;
        DDataAttribute versionTo = null;
        DDataAttribute discriminant = null;
        String discriminantValue = null;
        try {
            versionFrom = (DDataAttribute) clazz.getDeclaredField("VERSION_FROM").get(null);
            versionTo = (DDataAttribute) clazz.getDeclaredField("VERSION_TO").get(null);
            discriminant = (DDataAttribute) clazz.getDeclaredField("DISCR_ATTR").get(null);
            discriminantValue = (String) clazz.getDeclaredField("DISCR_VAL").get(null);
        } catch (IllegalAccessException | NoSuchFieldException ignore) {
        }
        String sql;
        if (versionFrom != null && versionTo != null) {
            if (version() == null) {
                sql = "t" + toTableIndex + "." + versionTo.getColumnName() + " IS NULL";
            } else {
                String timeSql = "CAST ('" + sqlTimestamp.format(version()) + "' AS TIMESTAMP)";
                sql = "(t" + toTableIndex + "." +
                        versionFrom.getColumnName() + " <= " + timeSql +
                        " AND (t" + toTableIndex + "." + versionTo.getColumnName() +
                        " > " + timeSql + " OR " +
                        "t" + toTableIndex + "." + versionTo.getColumnName() + " IS NULL))";
            }
        } else
            sql = "";

        if (discriminant != null)
            sql = sql + (sql.length() > 0 ? " AND " : "") + "t" +
                    toTableIndex + "." + discriminant.getColumnName() + "=" +
                    (String.class.isInstance(discriminant.getJavaType()) ?
                            "'" + discriminantValue + "'" :
                            discriminantValue);

        return sql;
    }
}
