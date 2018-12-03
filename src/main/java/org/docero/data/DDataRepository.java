package org.docero.data;

import java.io.Serializable;

public interface DDataRepository<T extends Serializable, C extends Serializable> {
    /**
     * Create new bean of class implements repository interface with default values and without serialization to database
     * @return created bean
     */
    <A extends T> A create();

    /**
     * Get database entry as bean by it primary key (mybatis bean is linked to database)
     * @param id primary key object
     * @return bean constructed from database entry
     */
    <A extends T> A get(C id);

    /**
     * Insert database entry from bean implementing repository interface, return
     * bean constructed from database entry (mybatis bean is linked to database)
     * @param bean these values used for update
     * @return bean constructed from database entry
     */
    <A extends T> A  insert(T bean);

    /**
     * Update database entry by bean implementing repository interface, return
     * bean constructed from database entry (mybatis bean is linked to database)
     * @param bean these values used for update
     * @return bean constructed from database entry
     */
    <A extends T> A  update(T bean);

    /**
     * Delete database entry by it primary key
     * @param id primary key object
     */
    void delete(C id);
}
