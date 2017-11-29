package org.docero.data.utils;

import java.util.HashSet;

public class DDataTypes {
    private static final HashSet<String> unmaskedTypes = new HashSet<String>() {{
        this.add("BOOLEAN");
        this.add("SMALLINT");
        this.add("INTEGER");
        this.add("BIGINT");
        this.add("REAL");
        this.add("DOUBLE");
        this.add("NUMERIC");
    }};

    public static String maskedValue(String jdbcType, String value) {
        if (unmaskedTypes.contains(jdbcType)) return value;
        if ("DATE".equals(jdbcType)) return "CAST('" + value + "' AS DATE)";
        if ("TIME".equals(jdbcType)) return "CAST('" + value + "' AS TIME)";
        if ("TIMESTAMP".equals(jdbcType)) return "CAST('" + value + "' AS TIMESTAMP)";
        return "'" + value + "'";
    }
}
