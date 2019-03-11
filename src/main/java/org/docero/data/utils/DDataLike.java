package org.docero.data.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class DDataLike {
    public static String in(String val) {
        if (val == null) return null;
        else if (val.startsWith("%") || val.endsWith("%")) return val;
        return "%" + val + "%";
    }

    public static String[] has(String val) {
        if (val == null) return null;
        String[] a = val.split(" ");
        for (int i = 0; i < a.length; i++) a[i] = in(a[i]);
        return a;
    }

    public static String ends(String val) {
        if (val == null) return null;
        else if (val.startsWith("%")) return val;
        return "%" + val;
    }

    public static String starts(String val) {
        if (val == null) return null;
        else if (val.endsWith("%")) return val;
        return val + "%";
    }

    public static String similar(Object val) {
        if (val == null) return "";
        if (val instanceof String)
            return Arrays.asList(((String) val).split(",")).stream().collect(Collectors.joining("|"));
        if (val instanceof Collection)
            return ((Collection<String>)val).stream().collect(Collectors.joining("|"));
        if (val.getClass().isArray())
            return (Arrays.stream((String[])val).collect(Collectors.joining("|")));
        return "";
    }
}
