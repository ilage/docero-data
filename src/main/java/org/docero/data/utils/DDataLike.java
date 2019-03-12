package org.docero.data.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static String similar(Iterable<String> val) {
        if (val == null) return "";
        Iterator<String> i = val.iterator();
        StringBuilder b = new StringBuilder();
        while (i.hasNext()) b.append('|').append(i.next());
        return b.length() > 0 ? b.substring(1) : "";
    }
}
