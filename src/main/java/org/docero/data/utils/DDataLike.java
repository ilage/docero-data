package org.docero.data.utils;

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
}
