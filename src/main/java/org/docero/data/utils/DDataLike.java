package org.docero.data.utils;

import java.util.Iterator;

public class DDataLike {
    public static String in(String val) {
        if (val == null) return null;
        else if (val.startsWith("%") || val.endsWith("%")) return val;
        return "%" + val + "%";
    }

    // for similar to
    public static String in(Iterable<String> val) {
        if (val == null) return null;
        Iterator<String> i = val.iterator();
        StringBuilder b = new StringBuilder();
        while (i.hasNext()) {
            String v = i.next();
            b.append('|').append(v.startsWith("%") || v.endsWith("%") ? v : "%" + v + "%");
        }
        return b.length() > 0 ? b.substring(1) : null;
    }

    // not used
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

    // for similar to
    public static String ends(Iterable<String> val) {
        if (val == null) return null;
        Iterator<String> i = val.iterator();
        StringBuilder b = new StringBuilder();
        while (i.hasNext()) {
            String v = i.next();
            b.append('|').append(v.startsWith("%") ? v : "%" + v);
        }
        return b.length() > 0 ? b.substring(1) : null;
    }

    public static String starts(String val) {
        if (val == null) return null;
        else if (val.endsWith("%")) return val;
        return val + "%";
    }

    // for similar to
    public static String starts(Iterable<String> val) {
        if (val == null) return null;
        Iterator<String> i = val.iterator();
        StringBuilder b = new StringBuilder();
        while (i.hasNext()) {
            String v = i.next();
            b.append('|').append(v.endsWith("%") ? v : v + "%");
        }
        return b.length() > 0 ? b.substring(1) : null;
    }
}
