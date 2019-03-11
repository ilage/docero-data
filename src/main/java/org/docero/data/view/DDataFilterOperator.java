package org.docero.data.view;

import java.util.Arrays;

public enum DDataFilterOperator {
    LESS_OR_EQUALS(1, "<="), GREATE_OR_EQUALS(1, ">="), LESS(1, "<"), GREATE(1, ">"), EQUALS(1, "="),
    NOT_EQUALS(1, "<>", true, false),
    IS_NULL(0, "IS NULL"), IS_NOT_NULL(0, "IS NOT NULL"),
    COUNT(0, "COUNT", false, true), MAX(0, "MAX", false, true), MIN(0, "MIN", false, true),
    AVG(0, "AVG", false, true), SUM(0, "SUM", false, true),
    LIKE(1, "LIKE"), STARTS(1, "LIKE"), NOT_LIKE(1, "NOT LIKE", true, false), NOT_STARTS(1, "NOT LIKE", true, false),
    LIKE_IGNORE_CASE(1, "LIKE"), STARTS_IGNORE_CASE(1, "LIKE"), NOT_LIKE_IGNORE_CASE(1, "NOT LIKE", true, false), NOT_STARTS_IGNORE_CASE(1, "NOT LIKE", true, false),
    SIMILAR_TO(1, "SIMILAR TO", false, false), NOT_SIMILAR_TO(1, "NOT SIMILAR TO", false, false),
    IN(1, "IN", false, false), NOT_IN(1, "NOT IN", false, false), BETWEEN(2, ""), EXPRESSION(99,"");

    private final int operands;
    private final String sql;
    private final boolean allowsNull;
    private final boolean aggregation;

    DDataFilterOperator(int operands, String sql) {
        this.operands = operands;
        this.sql = sql;
        this.allowsNull = false;
        this.aggregation = false;
    }

    DDataFilterOperator(int operands, String sql, boolean mayBeNull, boolean isAggregation) {
        this.operands = operands;
        this.sql = sql;
        this.allowsNull = mayBeNull;
        this.aggregation = isAggregation;
    }

    public int getOperands() {
        return operands;
    }

    @Override
    public String toString() {
        return sql;
    }

    @SuppressWarnings("unused")
    public static DDataFilterOperator fromString(String str) {
        return str == null ? null :
                Arrays.stream(DDataFilterOperator.values())
                        .filter(v -> str.equalsIgnoreCase(v.name()))
                        .findAny().orElse(
                        Arrays.stream(DDataFilterOperator.values())
                                .filter(v -> str.equalsIgnoreCase(v.sql))
                                .findAny().orElse(null)
                );
    }

    public boolean isAllowsNull() {
        return allowsNull;
    }

    public boolean isAggregation() {
        return aggregation;
    }
}
