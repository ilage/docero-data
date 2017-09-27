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
     * Is id column, bean can contains no one or number of id properties,
     * default false.
     * @return
     */
    boolean id() default false;

    /**
     * Column contains version part of table composite key used in DDataVersionalBean
     * @return
     */
    boolean versionData() default false;

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
}
