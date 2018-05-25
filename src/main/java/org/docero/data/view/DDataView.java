package org.docero.data.view;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.GeneratedValue;
import org.docero.data.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class DDataView extends AbstractDataView {
    private final static Logger LOG = LoggerFactory.getLogger(DDataView.class);

    private final SqlSession sqlSession;
    final Class<? extends DDataAttribute>[] roots;
    final DDataFilter[] columns;
    private DDataFilter filter = new DDataFilter();
    private final Temporal version;

    final static Comparator<DDataAttribute> propertiesComparator = Comparator.comparing(DDataAttribute::getPropertyName);
    final static Comparator<DDataFilter> columnsComparator = Comparator.comparing(k -> k.getAttribute().getPropertyName());
    private final HashMap<String, DDataAttribute> viewEntities = new HashMap<>();
    final IdentityHashMap<DDataFilter, String> viewPaths = new IdentityHashMap<>();
    final List<Sort> sortedPaths = new ArrayList<>();

    final HashMap<DDataAttribute, Set<DDataAttribute>> viewEIds = new HashMap<>();
    final HashMap<DDataAttribute, Set<DDataFilter>> viewProperties = new HashMap<>();

    DDataView(SqlSession sqlSession, Class[] roots, DDataFilter[] columns, Temporal version) {
        this.sqlSession = sqlSession;
        this.roots = roots;
        this.columns = columns;
        if (columns.length > 0 && Arrays.stream(columns).noneMatch(c -> c.isSortAscending() != null))
            columns[0].setSortAscending(true);
        this.version = version;

        for (Field field : roots[0].getDeclaredFields())
            if (field.isEnumConstant())
                try {
                    DDataAttribute idAtr = (DDataAttribute) field.get(null);
                    if (idAtr.isPrimaryKey()) {
                        viewEIds.computeIfAbsent(null, k -> new TreeSet<>(propertiesComparator))
                                .add(idAtr);
                    }
                } catch (IllegalAccessException ignore) {
                }
        for (DDataFilter column : columns) fillViewEntities(column, null, null);
    }

    private void fillViewEntities(DDataFilter column, String path, DDataAttribute parent) {
        DDataAttribute attribute = column.getAttribute();
        if (attribute != null) {
            String nameInPath = attribute.getPropertyName();
            String cp = path == null ? nameInPath : (path + "." + nameInPath);
            viewPaths.put(column, cp);
            //String[] attrPath = cp.split("\\.");
            if (attribute.isMappedBean()) {
                viewEntities.put(cp, attribute);
                for (Field field : attribute.getJavaType().getDeclaredFields())
                    if (field.isEnumConstant())
                        try {
                            DDataAttribute idAtr = (DDataAttribute) field.get(null);
                            if (idAtr.isPrimaryKey()) {
                                viewEIds.computeIfAbsent(attribute, k -> new TreeSet<>(propertiesComparator))
                                        .add(idAtr);
                            }
                        } catch (IllegalAccessException ignore) {
                        }
            } else if (!attribute.isPrimaryKey() && attribute.getColumnName() != null) {
                if (column.isSortAscending() != null)
                    sortedPaths.add(new Sort(cp, column.isSortAscending()));

                viewProperties.computeIfAbsent(parent,
                        k -> new TreeSet<>(columnsComparator))
                        .add(column);
            }

            if (column.getFilters() != null) column.getFilters()
                    .forEach(f -> fillViewEntities(f, cp, attribute));
        } else if (column.getFilters() != null) column.getFilters()
                .forEach(f -> fillViewEntities(f, path, parent));
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
        DSQL sql = buildFrom(roots[0]);
        sql.SELECT("COUNT(*)");
        buildFilters(sql);

        return sqlSession.selectOne("org.docero.data.selectCount",
                Collections.singletonMap("sqlStatement", sql.toString()));
    }

    public DDataViewRows select(int offset, int limit) throws DDataException {
        this.updates = new HashMap<>();
        DSQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column);
                    break;
                }
        super.addRootIdsToViewSql(sql);
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
        return new DDataViewRows(this, resultMap);
    }

    @SuppressWarnings("unchecked")
    public int[] aggregateInt(DDataFilterOperator operator) throws DDataException {
        DSQL agSql = new DSQL();
        agSql.SELECT("'group' as \"dDataBeanKey_\"");
        DSQL sql = buildFrom(roots[0]);
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, root, column);
                    String pathName = column.getName();
                    agSql.SELECT(operator + "(t.\"" + pathName + "\") AS \"" + pathName + "\"");
                    break;
                }
        buildFilters(sql);
        sql.GROUP_BY(keySql);
        agSql.FROM("(" + sql.toString() + ") AS t");
        Map<Object, Object> result = sqlSession.selectMap(
                "org.docero.data.selectView",
                Collections.singletonMap("sqlStatement", agSql.toString()), "dDataBeanKey_");
        Map<Object, Object> row = (Map<Object, Object>) result.get("group");
        if (row == null) return new int[0];
        int[] ret = new int[row.size() - 1];
        for (int i = 0; i < columns.length; i++) {
            Object ro = row.get(columns[i].getName());
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

    private void buildFilters(DSQL sql) throws DDataException {
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

        super.addFilterSql(sql, allTypesFilter, roots[0]);
        String vc = versionConstraint(roots[0], 0);
        if (vc.length() > 0) sql.WHERE(vc);

        DSQL ssql = new DSQL();
        for (int i = 0; i < roots.length; i++) {
            if (i > 0) ssql.OR();

            Class multiTypeClass = roots[i];
            String tc = typeConstraint(multiTypeClass, 0);
            if (tc.length() > 0) ssql.WHERE(tc);

            super.addFilterSql(ssql, someTypesFilter, multiTypeClass);
        }
        sql.WHERE(ssql);
    }

    /**
     * row -> beanPath -> index -> parameter entityPropertyPath
     */
    private HashMap<DDataViewRow, HashMap<String, Set<Integer>>> updates;

    DDataAttribute getEntityForPath(String s) {
        return viewEntities.get(s);
    }

    String getPathForColumn(DDataFilter column) {
        return viewPaths.get(column);
    }

    void addUpdate(DDataViewRow dDataViewRow, int index, String path) {
        int i = path.lastIndexOf('.');
        String beanPath = i < 0 ? null : path.substring(0, i);
        HashMap<String, Set<Integer>> update =
                updates.computeIfAbsent(dDataViewRow, k -> new HashMap<>());
        update.computeIfAbsent(beanPath, k -> new HashSet<>()).add(index);
    }

    public void flushUpdates(DDataExceptionHandler exceptionHandler) throws SQLException, DDataException {
        if (updates == null || updates.size() == 0) return;
        Date dateNow = new Date();

        Connection connection = sqlSession.getConnection();
        Set<PreparedUpdates> prepared = new TreeSet<>(Comparator.comparingInt(k -> k.entityPropertyPath.length()));
        try {
            // at first, process rows data by known bean update services (DDataBeanUpdateService)
            // they must do real updates in database and may replace ids of managed bean
            for (DDataViewRow row : updates.keySet())
                try {
                    HashMap<String, Set<Integer>> updatedEntities = updates.get(row);
                    HashMap<String, Set<Integer>> updatedParents = new HashMap<>();
                    for (String entityPropertyPath : updatedEntities.keySet()) {
                        DDataBeanUpdateService beanService = getUpdateServiceFor(entityPropertyPath);
                        if (beanService != null)
                            for (Integer updatedIndex : updatedEntities.get(entityPropertyPath))
                                if (beanService.update(row, updatedIndex, entityPropertyPath)) {
                                    String parentPath = entityPropertyPath.substring(0, entityPropertyPath.lastIndexOf('.'));
                                    if (!updatedEntities.containsKey(parentPath))
                                        updatedParents.computeIfAbsent(parentPath, k -> new HashSet<>())
                                                .add(getEntityForPath(parentPath).isCollection() ? 0 : updatedIndex);
                                }
                    }
                    // if children updates/inserts modify parent mapping attributes, add parent to updates
                    updatedEntities.putAll(updatedParents);
                } catch (Exception e) {
                    exceptionHandler.handle(e);
                }
            // next, write updated rows to database
            for (DDataViewRow row : updates.keySet())
                try {
                    HashMap<String, Set<Integer>> updatedEntities = updates.get(row);
                    HashMap<String, Set<Integer>> updatedParents = new HashMap<>();
                    for (String entityPropertyPath : updatedEntities.keySet()) {
                        DDataAttribute entityBeanAttribute = this.getEntityForPath(entityPropertyPath);
                        DDataBeanUpdateService beanService = getUpdateServiceFor(entityPropertyPath);
                        if (beanService == null || beanService.serviceDoesNotMakeUpdates()) {
                            PreparedUpdates pk = prepared.stream()
                                    .filter(k -> entityPropertyPath.equals(k.entityPropertyPath))
                                    .findAny().orElse(null);
                            if (pk == null)
                                prepared.add(pk = new PreparedUpdates(this, entityPropertyPath, connection));

                            for (Integer updatedIndex : updatedEntities.get(entityPropertyPath)) {
                                String firstIdProp = pk.getFirstIdColumnName();
                                Object anyId = row.getColumnValue(updatedIndex,
                                        entityPropertyPath.length() == 0 ? firstIdProp :
                                                entityPropertyPath + "." + firstIdProp);
                                boolean parentMustBeUpdated = false;
                                if (idIsNull(anyId)) { //is new element
                                    if (beanService == null) {
                                        for (DDataAttribute idAttribute : row.view.viewEIds.get(entityBeanAttribute))
                                            fillIdAttibute(connection, row,
                                                    entityBeanAttribute.getBeanInterface(),
                                                    entityPropertyPath,
                                                    updatedIndex, idAttribute, dateNow);
                                        pk.fillMappings(row, entityBeanAttribute, updatedIndex, entityPropertyPath);
                                    }
                                    pk.fillInsert(row, updatedIndex, dateNow);
                                    parentMustBeUpdated = entityBeanAttribute.joinMapping()
                                            .keySet().stream().anyMatch(v ->
                                                    Arrays.stream(entityBeanAttribute.getClass().getEnumConstants())
                                                            .filter(a -> v.equals(a.getColumnName()))
                                                            .anyMatch(a -> !a.isPrimaryKey())
                                            );
                                } else
                                    pk.fillUpdate(row, updatedIndex, dateNow);
                                if (parentMustBeUpdated) {
                                    String parentPath = entityPropertyPath.substring(0, entityPropertyPath.lastIndexOf('.'));
                                    if (!updatedEntities.containsKey(parentPath))
                                        updatedParents.computeIfAbsent(parentPath, k -> new HashSet<>())
                                                .add(getEntityForPath(parentPath).isCollection() ? 0 : updatedIndex);
                                }
                            }
                        }
                    }
                    // if children inserts modify parent mapping attributes, update parents entities
                    for (String entityPropertyPath : updatedParents.keySet()) {
                        DDataAttribute entityBeanAttribute = this.getEntityForPath(entityPropertyPath);
                        DDataBeanUpdateService beanService = getUpdateServiceFor(entityPropertyPath);
                        if (beanService == null || beanService.serviceDoesNotMakeUpdates()) {
                            PreparedUpdates pk = prepared.stream()
                                    .filter(k -> entityPropertyPath.equals(k.entityPropertyPath))
                                    .findAny().orElse(null);
                            if (pk == null)
                                prepared.add(pk = new PreparedUpdates(this, entityPropertyPath, connection));

                            for (Integer updatedIndex : updatedParents.get(entityPropertyPath))
                                pk.fillUpdate(row, updatedIndex, dateNow);
                        }
                    }
                } catch (Exception e) {
                    exceptionHandler.handle(e);
                }
            for (PreparedUpdates pk : prepared) pk.execute();
        } finally {
            for (PreparedUpdates pk : prepared) pk.close();
            //sqlSession.clearCache();
        }
    }

    static boolean idIsNull(Object anyId) {
        return anyId == null || (
                anyId instanceof Number && ((Number) anyId).longValue() == 0
        );
    }

    private void fillIdAttibute(
            Connection connection, DDataViewRow row, Class<? extends Serializable> beanInterface,
            String entityPropertyPath, Integer index, DDataAttribute idAttribute, Date dateNow
    ) throws NoSuchMethodException, SQLException {
        String pCase = Character.toUpperCase(idAttribute.getPropertyName().charAt(0)) +
                idAttribute.getPropertyName().substring(1);
        Method idProp = beanInterface.getDeclaredMethod("get" + pCase);
        String idPropertyPath = entityPropertyPath.length() == 0 ?
                idAttribute.getPropertyName() :
                entityPropertyPath + "." + idAttribute.getPropertyName();
        GeneratedValue gen = idProp.getAnnotation(GeneratedValue.class);
        if (gen == null) try {
            idProp = beanInterface.getDeclaredMethod("set" + pCase);
            gen = idProp.getAnnotation(GeneratedValue.class);
        } catch (Exception ignore) {
            gen = null;
        }
        if (gen == null) {
            if (Temporal.class.isAssignableFrom(idAttribute.getJavaType()))
                row.setColumnValue(dateNow, index, idPropertyPath, false);
            else if (Date.class.isAssignableFrom(idAttribute.getJavaType()))
                row.setColumnValue(dateNow, index, idPropertyPath, false);
        } else {
            Object idValue = null;
            switch (gen.strategy()) {
                case SEQUENCE:
                    try (PreparedStatement st = connection.prepareStatement("SELECT nextval(?);")) {
                        st.setString(1, gen.value());
                        try (ResultSet rs = st.executeQuery()) {
                            if (rs.next()) idValue = idFromResult(rs, idAttribute);
                        }
                    }
                    break;
                case SELECT:
                    try (Statement st = connection.createStatement()) {
                        try (ResultSet rs = st.executeQuery(gen.value())) {
                            if (rs.next()) idValue = idFromResult(rs, idAttribute);
                        }
                    }
                    break;
            }
            if (idValue != null)
                row.setColumnValue(idValue, index, idPropertyPath, false);
        }
    }

    private Object idFromResult(ResultSet rs, DDataAttribute idAttribute) throws SQLException {
        if (Integer.class.isAssignableFrom(idAttribute.getJavaType()))
            return rs.getInt(1);
        if (Long.class.isAssignableFrom(idAttribute.getJavaType()))
            return rs.getLong(1);
        if (Short.class.isAssignableFrom(idAttribute.getJavaType()))
            return rs.getShort(1);
        return null;
    }

    @SuppressWarnings("unchecked")
    private DDataBeanUpdateService getUpdateServiceFor(String entityPropertyPath) {
        DDataAttribute entityBeanAttribute = getEntityForPath(entityPropertyPath);
        return entityBeanAttribute == null ? null : getUpdateServiceFor(entityBeanAttribute.getBeanInterface());
    }

    private HashMap<Class, DDataBeanUpdateService> knownUpdaters = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <T extends Serializable> DDataBeanUpdateService<T> getUpdateServiceFor(Class<T> beanInterface) {
        return (DDataBeanUpdateService<T>) knownUpdaters.get(beanInterface);
    }

    public <T extends Serializable> void addUpdateService(Class<T> i, DDataBeanUpdateService<T> service) {
        knownUpdaters.put(i, service);
    }

    static class Sort {
        final String path;
        final boolean asc;

        Sort(String path, boolean asc) {
            this.path = path;
            this.asc = asc;
        }
    }
}
