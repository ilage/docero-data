package org.docero.data.view;

import org.apache.ibatis.jdbc.SQL;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.docero.data.utils.DDataException;
import org.docero.data.utils.DDataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.Temporal;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class DDataView extends AbstractDataView {
    private final static Logger LOG = LoggerFactory.getLogger(DDataView.class);

    private final SqlSession sqlSession;
    private final Class[] roots;
    private final DDataFilter[] columns;
    private DDataFilter filter = new DDataFilter();
    private final Temporal version;

    DDataView(SqlSession sqlSession, Class[] roots, DDataFilter[] columns, Temporal version) {
        this.sqlSession = sqlSession;
        this.roots = roots;
        this.columns = columns;
        this.version = version;
    }

    Temporal version() {
        return version;
    }

    public DDataFilter getFilter() {
        return filter;
    }

    public void setFilter(DDataFilter filter) {
        this.filter = filter;
    }

    public long count() throws DDataException {
        SQL sql = buildFrom(roots[0]);
        sql.SELECT("COUNT(*)");
        buildFilters(sql);

        return sqlSession.selectOne("org.docero.data.selectCount",
                Collections.singletonMap("sqlStatement", sql.toString()));
    }

    public Map<Object, Object> select(int offset, int limit) throws DDataException {
        SQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column, "", "", 0);
                    break;
                }
        buildFilters(sql);

        //if(LOG.isDebugEnabled()) LOG.debug("Preparing: "+sql.toString());
        Map<Object, Object> resultMap = sqlSession.selectMap(
                "org.docero.data.selectView",
                Collections.singletonMap("sqlStatement", sql.toString()), "dDataBeanKey_",
                new RowBounds(offset, limit));
        //if(LOG.isDebugEnabled()) LOG.debug("Total: "+resultMap.size());
        String in_condition = keySql + " IN (" + resultMap.keySet().stream()
                .map(k -> DDataTypes.maskedValue(getKeyType(), k.toString()))
                .collect(Collectors.joining(",")) +
                ")";
        for (SQL subSelect : getSubSelects()) {
            subSelect.WHERE(in_condition);
            //if(LOG.isDebugEnabled()) LOG.debug("Preparing: "+subSelect.toString());
            List<Map<Object, Object>> subResult = sqlSession.selectList(
                    "org.docero.data.selectView",
                    Collections.singletonMap("sqlStatement", subSelect.toString()));
            //if(LOG.isDebugEnabled()) LOG.debug("Total: "+subResult.size());
            for (Map<Object, Object> row : subResult) {
                Object key = row.get("dDataBeanKey_");
                mergeResultMaps(resultMap.get(key), row);
            }
        }
        return resultMap;
    }

    @SuppressWarnings("unchecked")
    private void mergeResultMaps(Object to, Object from) {
        if (from == null || to == null) return;
        if (!(from instanceof Map && to instanceof Map)) return;
        Map<Object, Object> target = (Map<Object, Object>) to;
        Map<Object, Object> source = (Map<Object, Object>) from;
        for (Object key : source.keySet()) {
            Object trgVal = target.get(key);
            Object srcVal = source.get(key);
            if (trgVal != null && srcVal != null) {
                if (trgVal instanceof Map) {
                    mergeResultMaps(trgVal, srcVal);
                } else {
                    if (trgVal instanceof List)
                        ((List) trgVal).add(srcVal);
                    else if (!(srcVal instanceof List)) {
                        ArrayList<Object> newListValue = new ArrayList<>();
                        newListValue.add(trgVal);
                        newListValue.add(srcVal);
                        target.put(key, newListValue);
                    }
                }
            } else target.put(key, srcVal);
        }
    }

    private void buildFilters(SQL sql) throws DDataException {
        for (int i = 0; i < roots.length; i++) {
            if (i > 0) sql.OR();

            Class multiTypeClass = roots[i];
            String verSql = versionAndTypeConstraint(multiTypeClass, 0);
            if (verSql.length() > 0) sql.WHERE(verSql);

            super.addFilterSql(sql, filter, multiTypeClass, "", 0);
        }
    }
}
