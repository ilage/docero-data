package org.docero.data.view;

import org.apache.ibatis.jdbc.SQL;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataTypes;

import java.lang.reflect.Field;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
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
            DDataFilter column, String path, int fromTableIndex
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

            if (attribute.isMappedBean()) {
                Integer tableIndex = usedCols.get(pathAttributeKey);
                if (tableIndex == null) {
                    tableIndex = tablesCounter.incrementAndGet();
                    int finalN = tableIndex;
                    String joinSql = attribute.joinTable() + " t" + finalN + " ON (" +
                            attribute.joinMapping().entrySet().stream()
                                    .map(m -> "t" + fromTableIndex + "." + m.getKey() +
                                            "=t" + finalN + "." + m.getValue())
                                    .collect(Collectors.joining(" AND "));
                    String verSql = versionalConstraint(attribute.getJavaType(), finalN);
                    sql.LEFT_OUTER_JOIN(joinSql + (verSql.length() > 0 ? " AND " + verSql : "") + ")");
                    usedCols.put(pathAttributeKey, tableIndex);
                }

                if (attribute.getJavaType().isEnum() && !attribute.isCollection() && column.hasFilters()) {

                    for (DDataFilter col : column.getFilters()) {
                        addColumnToViewSql(sql, usedCols, attribute.getJavaType(), col,
                                pathAttributeKey, tableIndex);
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

    void addFilterSql(SQL sql, List<DDataFilter> filters, Map<String, Integer> usedCols, String path, final int fromTableIndex) {
        if (filters == null) return;

        for (DDataFilter filter : filters) {
            if (!filter.getAttribute().isCollection()) {
                if (filter.getOperator() != null && !filter.hasFilters()) {
                    String columnReference = "t" + fromTableIndex + ".\"" +
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
                    String key = path + filter.getAttribute().getPropertyName() + PROP_PATCH_DELIMITER;
                    Integer tableIndex = usedCols.get(key);
                    if (tableIndex == null) {
                        tableIndex = tablesCounter.incrementAndGet();
                        final int finalN = tableIndex;
                        String joinSql = filter.getAttribute().joinTable() + " t" + tableIndex + " ON (" +
                                filter.getAttribute().joinMapping().entrySet().stream()
                                        .map(m -> "t" + fromTableIndex + "." + m.getKey() +
                                                "=t" + finalN + "." + m.getValue())
                                        .collect(Collectors.joining(" AND "));
                        String verSql = versionalConstraint(filter.getAttribute().getJavaType(), tableIndex);
                        sql.LEFT_OUTER_JOIN(joinSql + (verSql.length() > 0 ? " AND " + verSql : "") + ")");
                        usedCols.put(key, tableIndex);
                    }
                    addFilterSql(sql, filter.getFilters(), usedCols, key, tableIndex);
                }
            } else { // we can't use two separate filters by same table
                String key = path + filter.getAttribute().getPropertyName() + PROP_PATCH_DELIMITER;
                Integer tableIndex = usedCols.get(key);
                if (tableIndex == null) {
                    tableIndex = tablesCounter.incrementAndGet();
                    usedCols.put(key, tableIndex);
                }
                int finalN = tableIndex;
                sql.WHERE("EXISTS(" + new SQL() {{
                    SELECT("*");
                    FROM(filter.getAttribute().joinTable() + " t" + finalN);
                    WHERE(filter.getAttribute().joinMapping().entrySet().stream()
                            .map(m -> "t" + fromTableIndex + "." + m.getKey() + "=t" + finalN + "." + m.getValue())
                            .collect(Collectors.joining(" AND ")));
                    addFilterSql(this, filter.getFilters(), usedCols, key, finalN);
                }}.toString() + ")");
            }
        }
    }

    private final static DateTimeFormatter sqlTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    String versionalConstraint(Class clazz, int toTableIndex) {
        DDataAttribute versionFrom = null;
        DDataAttribute versionTo = null;
        try {
            versionFrom = (DDataAttribute) clazz.getDeclaredField("VERSION_FROM").get(null);
            versionTo = (DDataAttribute) clazz.getDeclaredField("VERSION_TO").get(null);
        } catch (IllegalAccessException | NoSuchFieldException ignore) {
        }

        if (versionFrom != null && versionTo != null) {
            if (version() == null) {
                return "t" + toTableIndex + "." + versionTo.getColumnName() + " IS NULL";
            } else {
                String timeSql = "CAST ('" + sqlTimestamp.format(version()) + "' AS TIMESTAMP)";
                return "(t" + toTableIndex + "." +
                        versionFrom.getColumnName() + " <= " + timeSql +
                        " AND (t" + toTableIndex + "." + versionTo.getColumnName() +
                        " > " + timeSql + " OR " +
                        "t" + toTableIndex + "." + versionTo.getColumnName() + " IS NULL))";
            }
        } else return "";
    }
}
