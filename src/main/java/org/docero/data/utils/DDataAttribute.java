package org.docero.data.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public interface DDataAttribute extends DDataBasicAttribute {
    /**
     * @return name of joined table of associated ddata-bean
     */
    String joinTable();

    /**
     * Joining properties. Size and order in joinBy and joinOn is same.
     *
     * @return names of columns of local table used for join
     */
    String[] joinBy();

    /**
     * Joining properties. Size and order in joinBy and joinOn is same.
     *
     * @return names of columns of joined table used for join
     */
    String[] joinOn();

    String readExpression();

    String writeExpression();

    /**
     * @return interface of associated(joined table) ddata-bean
     */
    Class<? extends Serializable> getBeanInterface();

    /**
     * Equals if same table, column and joining properties
     *
     * @param a1 first
     * @param a2 second
     * @return true if equals
     */
    static boolean equals(DDataAttribute a1, DDataAttribute a2) {
        return Objects.equals(a1.getColumnName(), a2.getColumnName())
                && Objects.equals(a1.joinTable(), a2.joinTable())
                && Arrays.equals(a1.joinBy(), a2.joinBy())
                && Arrays.equals(a1.joinOn(), a2.joinOn());
    }

    /**
     * Load enumeration for bean interface
     *
     * @param beanInterface interface of ddata-bean
     * @return enumeration (_WB_) contains all properties of bean including associated(inner) beans
     * @throws ClassNotFoundException if interface is not a ddata-bean(unknown)
     */
    @SuppressWarnings("unchecked")
    static Class<? extends DDataAttribute> loadEnum(Class<?> beanInterface) throws ClassNotFoundException {
        return (Class<? extends DDataAttribute>) DDataAttribute.class.getClassLoader()
                .loadClass(beanInterface.getCanonicalName() + "_WB_");
    }
}
