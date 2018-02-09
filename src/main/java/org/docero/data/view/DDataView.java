package org.docero.data.view;

import org.apache.ibatis.jdbc.SQL;
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
        String limitedSql = addBounds(sql.toString(), offset, limit);

        //if(LOG.isDebugEnabled()) LOG.debug("Preparing: "+sql.toString());
        Map<Object, Object> resultMap = sqlSession.selectMap(
                "org.docero.data.selectView",
                Collections.singletonMap("sqlStatement", limitedSql), "dDataBeanKey_");
        //if(LOG.isDebugEnabled()) LOG.debug("Total: "+resultMap.size());
        if (resultMap.size() > 0) {
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
                    mergeResultMaps(key, resultMap, row);
                }
            }
        }
        return resultMap;
    }

    private String addBounds(String s, int offset, int limit) {
        String limits = (offset > 0 ? " OFFSET " + offset : "") +
                (limit > 0 ? " LIMIT " + limit : "");
        if (s.endsWith(";")) {
            return s.substring(0, s.length() - 1) + limits + ";";
        } else {
            return s + limits;
        }
    }

    @SuppressWarnings("unchecked")
    private void mergeResultMaps(Object to_key, Map<Object, Object> to, Object leaf) {
        if (leaf == null || to_key == null) return;
        if (leaf instanceof Map && ((Map) leaf).containsKey("!")) {
            leaf = ((Map) leaf).get("!");
        }
        final Object finalLeaf = leaf;
        Object val = to.get(to_key);
        if (val == null) {
            to.put(to_key, new ArrayList<Object>() {{
                this.add(finalLeaf);
            }});
        } else if (val instanceof List) {
            ((List) val).add(finalLeaf);
        } else if (val instanceof Map && finalLeaf instanceof Map) {
            for (Object vk : ((Map) finalLeaf).keySet())
                if (!"dDataBeanKey_".equals(vk)) {
                    mergeResultMaps(vk, (Map<Object, Object>) val, ((Map) finalLeaf).get(vk));
                }
        } else {
            to.put(to_key, new ArrayList<Object>() {{
                this.add(val);
                this.add(finalLeaf);
            }});
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
