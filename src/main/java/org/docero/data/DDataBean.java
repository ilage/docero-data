package org.docero.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface DDataBean {
    /**
     * unique name used in myBatis map files
     * @return
     */
    String value() default "";

    /**
     * database schema
     * @return
     */
    String schema() default "";

    /**
     * table, view or procedure name used for type mapping
     * @return
     */
    String table() default "";

    /**
     * data growth rate
     * @return
     */
    TableGrowType growth() default TableGrowType.NORMAL;

    /**
     * entity is a dictionary and may be cached outside of myBatis cache
     * @return
     */
    DictionaryType dictionary() default DictionaryType.NO;
}
