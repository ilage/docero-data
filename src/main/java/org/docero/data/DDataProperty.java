package org.docero.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface DDataProperty {
    /**
     * Mapped column name, default (empty) lowercase property name.
     * @return
     */
    String value() default "";

    /**
     * Property must be ignored in entity mapping
     * @return
     */
    boolean Trancient() default false;

    /**
     * Is id column, bean can contains no one or number of id properties,
     * default false.
     * @return
     */
    boolean id() default false;

    /**
     * Column contains version part of table composite key used in DDataVersionalBean
     * @return
     */
    boolean versionFrom() default false;

    /**
     * Column contains upper restriction of version part used in DDataVersionalBean
     * @return
     */
    boolean versionTo() default false;

    /**
     * Is column may be NULL, default true.
     * @return
     */
    boolean nullable() default true;

    /**
     * Length for CHAR and VARCHAR types, default 0 (not controlled by framework)
     * @return
     */
    int length() default 0;

    /**
     * Sql statement used to read value, char '?' replaced by field name
     * @return sql
     */
    String reader() default "";

    /**
     * Sql statement used to write value, char '?' replaced by field name
     * @return sql
     */
    String writer() default "";
}
