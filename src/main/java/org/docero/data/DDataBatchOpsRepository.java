package org.docero.data;

import org.apache.ibatis.executor.BatchResult;

import java.io.Serializable;
import java.util.List;

public interface DDataBatchOpsRepository {

    <T extends Serializable> T create(Class<T> clazz);

    <T extends Serializable> T get(Class<T> clazz, Serializable id);

    void insert(Serializable bean);

    void update(Serializable bean);

    List<BatchResult> flushStatements();
}
