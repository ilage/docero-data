package org.docero.data.view;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.utils.DDataException;
import org.docero.data.utils.DDataTypes;
import org.docero.data.utils.DSQL;
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
        if (columns.length > 0 && Arrays.stream(columns).noneMatch(c -> c.isSortAscending() != null))
            columns[0].setSortAscending(true);
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
        HashSet<Integer> alreadyJoined = new HashSet<>();
        DSQL sql = buildFrom(roots[0]);
        sql.SELECT("COUNT(*)");
        buildFilters(sql, alreadyJoined);

        return sqlSession.selectOne("org.docero.data.selectCount",
                Collections.singletonMap("sqlStatement", sql.toString()));
    }

    public Map<Object, Object> select(int offset, int limit) throws DDataException {
        DSQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        HashSet<Integer> alreadyJoined = new HashSet<>();
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column, "", "", 0, alreadyJoined);
                    break;
                }
        buildFilters(sql, alreadyJoined);
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
            for (DSQL subSelect : getSubSelects()) {
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

    @SuppressWarnings("unchecked")
    public int[] aggregateInt(DDataFilterOperator operator) throws DDataException {
        DSQL agSql = new DSQL();
        agSql.SELECT("'group' as \"dDataBeanKey_\"");
        HashSet<Integer> alreadyJoined = new HashSet<>();
        DSQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column, "", "", 0, alreadyJoined);
                    String pathName = column.mapToName() != null ? column.mapToName() : column.getName();
                    agSql.SELECT(operator + "(t.\"" + pathName + "\") AS \"" + pathName + "\"");
                    break;
                }
        buildFilters(sql, alreadyJoined);
        sql.GROUP_BY(keySql);
        agSql.FROM("(" + sql.toString() + ") AS t");
        Map<Object, Object> result = sqlSession.selectMap(
                "org.docero.data.selectView",
                Collections.singletonMap("sqlStatement", agSql.toString()), "dDataBeanKey_");
        Map<Object, Object> row = (Map<Object, Object>) result.get("group");
        if (row == null) return new int[0];
        int[] ret = new int[row.size() - 1];
        for (int i = 0; i < columns.length; i++) {
            Object ro = row.get(columns[i].mapToName() != null ? columns[i].mapToName() : columns[i].getName());
            if (ro instanceof Number) ret[i] = ((Number) ro).intValue();
        }
        return ret;
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

    private void buildFilters(DSQL sql, HashSet<Integer> alreadyJoined) throws DDataException {
        DDataFilter allTypesFilter = new DDataFilter();
        allTypesFilter.getFilters().addAll(
                filter.getFilters().stream().filter(f ->
                        Arrays.stream(roots).allMatch(r -> isApplicable(r, f))
                ).collect(Collectors.toList())
        );
        DDataFilter someTypesFilter = new DDataFilter();
        someTypesFilter.getFilters().addAll(
                filter.getFilters().stream().filter(
                        f -> !allTypesFilter.getFilters().contains(f)
                ).collect(Collectors.toList())
        );

        super.addFilterSql(sql, allTypesFilter, roots[0], "", 0, alreadyJoined);
        String vc = versionConstraint(roots[0], 0);
        if (vc.length() > 0) sql.WHERE(vc);

        DSQL ssql = new DSQL();
        for (int i = 0; i < roots.length; i++) {
            if (i > 0) ssql.OR();

            Class multiTypeClass = roots[i];
            String tc = typeConstraint(multiTypeClass, 0);
            if (tc.length() > 0) ssql.WHERE(tc);

            super.addFilterSql(ssql, someTypesFilter, multiTypeClass, "", 0, alreadyJoined);
        }
        sql.WHERE(ssql);
    }
}
