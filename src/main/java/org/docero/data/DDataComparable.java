package org.docero.data;

public interface DDataComparable<T> extends Comparable<T> {
    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     * <p>
     * <p>This method compares only not id and simple type properties of data beans
     * (nor associations or collections of mapped beans)</p>
     * <p>
     * <p>This class ordering is inconsistent with equals.</p>
     *
     * @param obj the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    int compareSimpleTypes(T obj);

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     * <p>
     * <p>This method compares only not id properties of data beans, including
     * properties with mapped beans</p>
     * <p>
     * <p>Properties that do not support Comparable interface returns 0</p>
     * <p>
     * <p>This class ordering is inconsistent with equals.</p>
     *
     * @param obj the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    int compareTo(T obj);
}
