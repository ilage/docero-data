package org.docero.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for methods of DDataRepository, allows:<ul>
 *     <li>set load type for mapped entities (EAGER|LAZY)</li>
 *     <li>set custom SQL-queries, and alias for result table</li>
 *     <li>set custom mapper for result, but mapper must return entities with same type</li>
 * </ul>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface DDataFetch {
    /**
     * Load mapped entities in single select, default DDataFetchType.COLLECTIONS_ARE_LAZY
     * @return load type of mapped entities
     */
    DDataFetchType value() default DDataFetchType.COLLECTIONS_ARE_LAZY;

    /**
     * SQL query after FROM operator, can contains method parameter names (like <i>:parameterName</i>)
     * <p>
     * Can call stored procedure that returns table of DDataRepository data beans, or specified resultMap
     * </p>
     * @return SQL query after FROM operator
     */
    String from() default "";

    /**
     * Used with 'from' parameter
     * @return Table alias used in FROM operator, default empty
     */
    String alias() default "";

    /**
     * Used with 'from' parameter
     * @return Custom resultMap name used for mapping results
     */
    String resultMap() default "";
}
