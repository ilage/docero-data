package org.docero.data.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public interface DDataAttribute extends DDataBasicAttribute {
    String joinTable();

    String[] joinBy();

    String[] joinOn();

    String readExpression();

    String writeExpression();

    Class<? extends Serializable> getBeanInterface();

    public static boolean equals(DDataAttribute a1, DDataAttribute a2) {
        return Objects.equals(a1.getColumnName(), a2.getColumnName())
                && Objects.equals(a1.joinTable(), a2.joinTable())
                && Arrays.equals(a1.joinBy(), a2.joinBy())
                && Arrays.equals(a1.joinOn(), a2.joinOn());
    }
}
