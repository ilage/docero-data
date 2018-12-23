package org.docero.data;

import java.io.Serializable;

public abstract class AbstractRepository<T extends java.io.Serializable, C extends java.io.Serializable>
        implements org.docero.data.DDataRepository<T, C> {
    protected void cache(Serializable bean) {
        DData.cache(bean);
    }

    protected <U extends Serializable> java.util.List<U> listCached(
            Class<U> type, org.apache.ibatis.session.SqlSession session, String selectId) {
        return DData.list(type, session, selectId);
    }

    protected void updateVersion(Class<? extends Serializable> type) {
        DData.updateVersion(type);
    }

    protected void clearVersion(Class<? extends Serializable> type) {
        DData.clearVersion(type);
    }
}
