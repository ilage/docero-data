package org.docero.data.view;

import org.apache.ibatis.session.SqlSessionFactory;
import org.docero.data.DDataDictionariesService;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;
import org.mybatis.spring.support.SqlSessionDaoSupport;

import java.time.temporal.Temporal;
import java.util.List;

@SuppressWarnings({"unchecked", "unused"})
public class DDataViewBuilder extends SqlSessionDaoSupport {
    private DDataDictionariesService dictionariesService;

    public DDataViewBuilder(SqlSessionFactory sqlSessionFactory) {
        super.setSqlSessionFactory(sqlSessionFactory);
    }

    public void dictionariesService(DDataDictionariesService dictionariesService) {
        this.dictionariesService = dictionariesService;
    }

    public DDataView build(List<Class<? extends DDataAttribute>> alternativeRoots, List<DDataFilter> columns) throws DDataException {
        return new DDataView(
                getSqlSession(), dictionariesService,
                alternativeRoots.toArray(new Class[alternativeRoots.size()]),
                columns.toArray(new DDataFilter[columns.size()]),
                null
        );
    }

    public DDataView build(Class<? extends DDataAttribute> root, List<DDataFilter> columns) throws DDataException {
        return new DDataView(
                getSqlSession(), dictionariesService,
                new Class[]{root},
                columns.toArray(new DDataFilter[columns.size()]),
                null
        );
    }

    public DDataView build(Class<? extends DDataAttribute> root, DDataFilter... columns) throws DDataException {
        return new DDataView(
                getSqlSession(), dictionariesService,
                new Class[]{root},
                columns,
                null
        );
    }

    public DDataView build(List<Class<? extends DDataAttribute>> alternativeRoots, Temporal version, List<DDataFilter> columns) throws DDataException {
        return new DDataView(
                getSqlSession(), dictionariesService,
                alternativeRoots.toArray(new Class[alternativeRoots.size()]),
                columns.toArray(new DDataFilter[columns.size()]),
                version
        );
    }

    public DDataView build(Class<? extends DDataAttribute> root, Temporal version, List<DDataFilter> columns) throws DDataException {
        return new DDataView(
                getSqlSession(), dictionariesService,
                new Class[]{root},
                columns.toArray(new DDataFilter[columns.size()]),
                version
        );
    }

    public DDataView build(Class<? extends DDataAttribute> root, Temporal version, DDataFilter... columns) throws DDataException {
        return new DDataView(
                getSqlSession(), dictionariesService,
                new Class[]{root},
                columns,
                version
        );
    }
}
