package org.docero.data.view;

import org.apache.ibatis.session.SqlSession;
import org.docero.data.DDataDictionariesService;
import org.docero.data.GeneratedValue;
import org.docero.data.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
    private final DDataFilter[] columns;
    private DDataFilter filter = new DDataFilter();
    private final Temporal version;
    private boolean selectAllVersions;

    /*private final HashMap<String, DDataAttribute> tableEntities = new HashMap<>();
    final IdentityHashMap<DDataFilter, String> viewPaths = new IdentityHashMap<>();*/
    /*final HashMap<String, List<EntityMapping>> viewMappings = new HashMap<>();
    final List<Sort> sortedPaths = new ArrayList<>();*/

    DDataView(
            SqlSession sqlSession, DDataDictionariesService dictionariesService,
            Class<? extends DDataAttribute>[] roots,
            DDataFilter[] columns, Temporal version
    ) throws DDataException {
        super(roots, columns, dictionariesService);
        this.sqlSession = sqlSession;
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

    /**
     * Ignore version for versional beans. Default false.
     */
    @Override
    public boolean selectAllVersions() {
        return selectAllVersions;
    }

    public void selectAllVersions(boolean flag) {
        selectAllVersions = flag;
    }

    public long count() throws DDataException {
        DSQL sql = buildFrom();
        sql.SELECT("COUNT(*)");
        buildFilters(sql);

        return sqlSession.selectOne("org.docero.data.selectCount",
                Collections.singletonMap("sqlStatement", sql.toString()));
    }

    public String firstLevelSelect(int offset, int limit) throws DDataException {
        return firstLevelSelect(getKeySQL(), offset, limit);
    }

    private String firstLevelSelect(String keySql, int offset, int limit) throws DDataException {
        DSQL sql = buildFrom();

        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, column);
                    break;
                }
        super.addRootIdsToViewSql(sql);
        buildFilters(sql);
        return limit > 0 ? addBounds(sql.toString(), offset, limit) : sql.toString();
    }

    public DDataViewRows select(int offset, int limit) throws DDataException {
        this.updates = new HashMap<>();
        String keySql = getKeySQL();
        String limitedSql = firstLevelSelect(keySql, offset, limit);

        if (LOG.isDebugEnabled()) LOG.debug("Preparing: " + limitedSql);
        List<Map<String, Object>> resultMap = selectViewData(sqlSession, limitedSql);
        if (LOG.isDebugEnabled()) LOG.debug("Total: " + resultMap.size());

        if (resultMap.size() > 0) {
            String in_condition = keySql + " IN (" + resultMap.stream()
                    .map(v -> v.get("dDataBeanKey_"))
                    .map(k -> DDataTypes.maskedValue(getKeyType(), k.toString()))
                    .collect(Collectors.joining(",")) +
                    ")";
            for (DSQL subSelect : getSubSelects()) {
                subSelect.WHERE(in_condition);
                if (LOG.isDebugEnabled()) LOG.debug("Preparing: " + subSelect.toString());
                List<Map<String, Object>> subResult = selectViewData(sqlSession, subSelect.toString());
                if (LOG.isDebugEnabled()) LOG.debug("Total: " + subResult.size());

                if (!subResult.isEmpty())
                    for (Map<String, Object> row : subResult)
                        resultMap.stream()
                                .filter(m -> m.get("dDataBeanKey_").equals(row.get("dDataBeanKey_")))
                                .findFirst()
                                .ifPresent(p -> mergeSubSelect(p, row));
            }
        }
        return new DDataViewRows(this, resultMap);
    }

    @SuppressWarnings("unchecked")
    private void mergeSubSelect(Map<String, Object> dest, Map<String, Object> src) {
        for (Map.Entry<String, Object> e : src.entrySet()) {
            Object o = dest.get(e.getKey());
            if (e.getValue() instanceof Map) {
                if (o == null) {
                    List<Map<String, Object>> nl = new ArrayList<>();
                    dest.put(e.getKey(), nl);
                    nl.add((Map<String, Object>) e.getValue());
                } else if (o instanceof List) {
                    ((List) o).add(e.getValue());
                } else if (o instanceof Map) {
                    mergeSubSelect((Map<String, Object>) o, (Map<String, Object>) e.getValue());
                } else
                    dest.put(e.getKey(), e.getValue());
            } else
                dest.put(e.getKey(), e.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    public int[] aggregateInt(DDataFilterOperator operator) throws DDataException {
        DSQL agSql = new DSQL();
        agSql.SELECT("'group' as \"dDataBeanKey_\"");
        DSQL sql = buildFrom();
        String keySql = getKeySQL();
        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, column);
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
    private HashMap<DDataViewRow, TreeMap<String, Set<Integer>>> updates;

    void addUpdate(DDataViewRow dDataViewRow, int index, String path) {
        int i = path.lastIndexOf('.');
        String beanPath = i < 0 ? null : path.substring(0, i);
        TreeMap<String, Set<Integer>> update =
                updates.computeIfAbsent(dDataViewRow, k -> new TreeMap<>(
                        (s1, s2) -> Integer.compare(s2 == null ? 0 : s2.length(), s1 == null ? 0 : s1.length()))
                );
        update.computeIfAbsent(beanPath, k -> new HashSet<>()).add(index);
    }

    public void flushUpdates(DDataExceptionHandler exceptionHandler) throws SQLException, DDataException {
        if (updates == null || updates.size() == 0) return;
        final Date dateNow = TemporalDataOperations.Date.get();

        Connection connection = sqlSession.getConnection();
        Set<PreparedUpdates> prepared = new TreeSet<>(Comparator.comparingInt(k ->
                k.entityPropertyPath == null ? 0 : k.entityPropertyPath.length()));
        try {
            // at first, process rows data by known bean update services (DDataBeanUpdateService)
            // they must do real updates in database and may replace ids of managed bean
            for (DDataViewRow row : updates.keySet())
                try {
                    TreeMap<String, Set<Integer>> updatedEntities = updates.get(row);
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
                    TreeMap<String, Set<Integer>> updatedEntities = updates.get(row);
                    HashMap<String, Set<Integer>> updatedParents = new HashMap<>();
                    for (String entityPropertyPath : updatedEntities.keySet()) {
                        AbstractDataView.TableEntity entity = this.getEntityForPath(entityPropertyPath);
                        @SuppressWarnings("unchecked")
                        DDataBeanUpdateService beanService = getUpdateServiceFor(entityPropertyPath);
                        if (beanService == null || beanService.serviceDoesNotMakeUpdates()) {
                            PreparedUpdates pk = prepared.stream()
                                    .filter(k -> Objects.equals(entityPropertyPath, k.entityPropertyPath))
                                    .findAny().orElse(null);
                            if (pk == null)
                                prepared.add(pk = new PreparedUpdates(this, entityPropertyPath, connection));

                            for (Integer updatedIndex : updatedEntities.get(entityPropertyPath)) {
                                String firstIdProp = pk.getFirstIdColumnName();
                                Object anyId = row.getColumnValue(updatedIndex,
                                        entityPropertyPath == null ? firstIdProp :
                                                entityPropertyPath + "." + firstIdProp);
                                boolean parentMustBeUpdated = false;
                                if (idIsNull(anyId)) { //is new element
                                    if (beanService == null) {
                                        for (TableCell cell : entity.cells)
                                            if (cell.attribute.isPrimaryKey() && !cell.isVersion)
                                                fillIdAttibute(connection, row,
                                                        entity.beanInterface,
                                                        entityPropertyPath,
                                                        updatedIndex, cell, dateNow);
                                        if (entity != null)
                                            pk.fillMappings(row, entity, updatedIndex, entityPropertyPath);
                                    }
                                    pk.fillInsert(row, updatedIndex, dateNow);
                                    parentMustBeUpdated = entity.parent != null && entity.parent.mappings.entrySet()
                                            .stream().anyMatch(c -> !c.getKey().attribute.isPrimaryKey() &&
                                                    c.getValue().name.startsWith(entityPropertyPath + "."));
                                } else
                                    pk.fillUpdate(row, updatedIndex, dateNow);

                                if (parentMustBeUpdated) {
                                    int lii = entityPropertyPath.lastIndexOf('.');
                                    String parentPath = lii < 0 ? null : entityPropertyPath.substring(0, lii);
                                    if (!updatedEntities.containsKey(parentPath))
                                        updatedParents.computeIfAbsent(parentPath, k -> new HashSet<>())
                                                .add(getEntityForPath(parentPath).isCollection() ? 0 : updatedIndex);
                                }
                            }
                        }
                    }
                    // if children inserts modify parent mapping attributes, update parents entities
                    for (String entityPropertyPath : updatedParents.keySet()) {
                        //DDataAttribute entityBeanAttribute = this.getEntityForPath(entityPropertyPath);
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
            String entityPropertyPath, Integer index, TableCell cell, Date dateNow
    ) throws NoSuchMethodException, SQLException {
        String pCase = Character.toUpperCase(cell.attribute.getPropertyName().charAt(0)) +
                cell.attribute.getPropertyName().substring(1);
        Method idProp = beanInterface.getMethod("get" + pCase);
        String idPropertyPath = entityPropertyPath.length() == 0 ?
                cell.attribute.getPropertyName() :
                entityPropertyPath + "." + cell.attribute.getPropertyName();
        GeneratedValue gen = idProp.getAnnotation(GeneratedValue.class);
        if (gen == null) try {
            idProp = beanInterface.getMethod("set" + pCase);
            gen = idProp.getAnnotation(GeneratedValue.class);
        } catch (Exception ignore) {
            gen = null;
        }
        if (gen == null) {
            AbstractDataView.TableEntity entity = getEntityForPath(entityPropertyPath);
            entity.mappings.entrySet().stream()
                    .filter(m -> m.getValue().name.equals(idPropertyPath))
                    .findAny().ifPresent(m ->
                    row.setColumnValue(
                            row.getColumnValue(entity.isCollection() ? 0 : index, m.getKey().name),
                            index, idPropertyPath));
            if (Temporal.class.isAssignableFrom(cell.attribute.getJavaType()))
                row.setColumnValue(dateNow, index, idPropertyPath, false);
            else if (Date.class.isAssignableFrom(cell.attribute.getJavaType()))
                row.setColumnValue(dateNow, index, idPropertyPath, false);
        } else {
            Object idValue = null;
            switch (gen.strategy()) {
                case SEQUENCE:
                    try (PreparedStatement st = connection.prepareStatement("SELECT nextval(?);")) {
                        st.setString(1, gen.value());
                        try (ResultSet rs = st.executeQuery()) {
                            if (rs.next()) idValue = idFromResult(rs, cell.attribute);
                        }
                    }
                    break;
                case SELECT:
                    try (Statement st = connection.createStatement()) {
                        try (ResultSet rs = st.executeQuery(gen.value())) {
                            if (rs.next()) idValue = idFromResult(rs, cell.attribute);
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
        TableEntity entity = getEntityForPath(entityPropertyPath);
        return entity == null ? null : getUpdateServiceFor(entity.beanInterface);
    }

    private HashMap<Class, DDataBeanUpdateService> knownUpdaters = new HashMap<>();

    @SuppressWarnings({"unchecked", "WeakerAccess"})
    public <T extends Serializable> DDataBeanUpdateService<T> getUpdateServiceFor(Class<T> beanInterface) {
        return (DDataBeanUpdateService<T>) knownUpdaters.get(beanInterface);
    }

    public <T extends Serializable> void addUpdateService(Class<T> i, DDataBeanUpdateService<T> service) {
        knownUpdaters.put(i, service);
    }
}
