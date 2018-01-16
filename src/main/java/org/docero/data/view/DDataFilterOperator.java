package org.docero.data.view;

import java.util.Arrays;

public enum DDataFilterOperator {
    LESS_OR_EQUALS(1, "<="), GREATE_OR_EQUALS(1, ">="), LESS(1, "<"), GREATE(1, ">"), EQUALS(1, "="),
    NOT_EQUALS(1, "<>", true),
    IS_NULL(0, "IS NULL"), IS_NOT_NULL(0, "IS NOT NULL"),
    LIKE(1, "LIKE"), STARTS(1, "LIKE"),
    NOT_LIKE(1, "NOT LIKE", true), NOT_STARTS(1, "NOT LIKE", true),
    BETWEEN(2, "");

    private final int operands;
    private final String sql;
    private final boolean allowsNull;

    DDataFilterOperator(int operands, String sql) {
        this.operands = operands;
        this.sql = sql;
        this.allowsNull = false;
    }

    DDataFilterOperator(int operands, String sql, boolean mayBeNull) {
        this.operands = operands;
        this.sql = sql;
        this.allowsNull = mayBeNull;
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
}
