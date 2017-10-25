package org.docero.data;

import java.io.Serializable;
import java.time.temporal.Temporal;

/**
 * Bean what storey all modifications in table and have composite key from some id-value
 * and version value (type parameter)
 *
 * @param <A> any type what can be compared by greater and lover sql-operators, usually timestamp
 */
public interface DDataVersionalBean<A extends Temporal> extends Serializable {
    /**
     * Version value for versional bean, alias for property marked as actualFrom
     * @return version value
     */
    A getActualFrom_();

    /**
     * Set version of versional bean, alias for property marked as actualFrom
     * @param at version value
     */
    void setActualFrom_(A at);

    /**
     * Used in queries and not stored in database, version value passed to method
     * @return version value used in query
     */
    A getDDataBeanActualAt_();
}
