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
        if (value == null) return "NULL";
        if (unmaskedTypes.contains(jdbcType)) {
            if (value.length() > 127)
                throw new IllegalArgumentException("Invalid value for type " + jdbcType + ":" + value);
            return value.replaceAll("[';\"]", "");
        }
        String stringValue = value.replaceAll("[']", "''");
        if ("DATE".equals(jdbcType)) return "CAST('" + stringValue + "' AS DATE)";
        if ("TIME".equals(jdbcType)) return "CAST('" + stringValue + "' AS TIME)";
        if ("TIMESTAMP".equals(jdbcType)) return "CAST('" + stringValue + "' AS TIMESTAMP)";
        return "'" + stringValue + "'";
    }
}
