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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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

    /**
     * View data filter getter
     *
     * @return used filter
     */
    public DDataFilter getFilter() {
        return filter;
    }

    /**
     * View data filter setter
     *
     * @param filter used filter
     */
    public void setFilter(DDataFilter filter) {
        this.filter = filter;
    }

    /**
     * Ignore version for versional beans. Default false.
     *
     * @return is version fields are ignored
     */
    @Override
    public boolean selectAllVersions() {
        return selectAllVersions;
    }

    /**
     * Set ignore version for versional beans. Default false.
     *
     * @param flag do ignore version fields
     */
    public void selectAllVersions(boolean flag) {
        selectAllVersions = flag;
    }

    /**
     * Select rows count for view with applied filter
     *
     * @return count of records
     * @throws DDataException on any logical exceptions
     */
    public long count() throws DDataException {
        long result = 0;
        DSQL sql = buildFrom();
        sql.SELECT("COUNT(*)");
        buildFilters(sql);

        try (PreparedStatement pst = prepareStatement(sqlSession, sql.toString())) {
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) result = rs.getLong(1);
            }
        } catch (SQLException e) {
            LOG.error("exception in DDataView", e);
            throw new DDataException("JDBC: " + e.getMessage());
        }
        return result;
        /*return sqlSession.selectOne("org.docero.data.selectCount",
                Collections.singletonMap("sqlStatement", sql.toString()));*/
    }

    /**
     * Return SQL expression for select view data. View data may contains collections
     * in some columns, and then view use sub-selects, this method return only primary select.
     *
     * @param offset like a OFFSET in SQL (not used if limit=0)
     * @param limit  like a LIMIT in SQL but if 0 assumed as no limit
     * @return SQL expression
     * @throws DDataException on any logical exceptions
     */
    public String firstLevelSelect(int offset, int limit) throws DDataException {
        return firstLevelSelect(getKeySQL(), offset, limit);
    }

    private String firstLevelSelect(String keySql, int offset, int limit) throws DDataException {
        DSQL sql = buildFrom();

        sql.SELECT(keySql + " as \"dDataBeanKey_\"");
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, column, false);
                    break;
                }
        super.addRootIdsToViewSql(sql);
        buildFilters(sql);
        return limit > 0 ? addBounds(sql.toString(), offset, limit) : sql.toString();
    }

    /**
     * Select data for view with defined columns and applied filter
     *
     * @param offset like a OFFSET in SQL (not used if limit=0)
     * @param limit  like a LIMIT in SQL but if 0 assumed as no limit
     * @return rows object
     * @throws DDataException on any logical exceptions
     */
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

                String vc = versionConstraint(roots[0], 0);
                if (vc.length() > 0) subSelect.WHERE(vc);

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

    /**
     * Make DDataViewRows object used for inserting new records
     *
     * @return rows object
     */
    public DDataViewRows buildDataLoader() {
        this.updates = new HashMap<>();
        return new DDataViewRows(this, new ArrayList<Map<String, Object>>());
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

    /**
     * Создаёт агрегирующую таблицу по заданным в представлении колонкам, с использованием
     * переданной функции агрегирующей функции
     *
     * @param operator агрегирующая функция
     * @return массив значений агрегатов
     * @throws DDataException
     */
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
                    super.addColumnToViewSql(sql, column, true);
                    StringBuilder pathName = new StringBuilder(column.getName());
                    DDataFilter parent = column;
                    while (!parent.getFilters().isEmpty()) {
                        parent = parent.getFilters().get(0);
                        pathName.append(".").append(parent.getName());
                    }
                    agSql.SELECT(operator + "(t.\"" + pathName + "\") AS \"" + pathName + "\"");
                    break;
                }
        buildFilters(sql);
        sql.GROUP_BY(keySql);
        agSql.FROM("(" + sql.toString() + ") AS t");

        if (LOG.isDebugEnabled()) LOG.debug("Preparing: " + agSql.toString());
        List<Map<String, Object>> result = selectViewData(sqlSession, agSql.toString());
        if (LOG.isDebugEnabled()) LOG.debug("Total: " + result.size());
        if (result.isEmpty()) return new int[]{0};

        Map<String, Object> row = result.get(0);
        if (row == null) return new int[]{0};
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
        String beanPath = i < 0 ? "" : path.substring(0, i);
        TreeMap<String, Set<Integer>> update =
                updates.computeIfAbsent(dDataViewRow, k -> new TreeMap<>(Comparator.reverseOrder()));
        update.computeIfAbsent(beanPath, k -> new HashSet<>()).add(index);
    }

    /**
     * Write updates on view rows.
     *
     * @param exceptionHandler class for exceptions handling
     * @throws SQLException   on jdbc exceptions
     * @throws DDataException on any logical exceptions
     */
    public void flushUpdates(DDataExceptionHandler exceptionHandler) throws SQLException, DDataException {
        if (updates == null || updates.size() == 0) return;
        final Date dateNow = DMLOperations.date();

        Connection connection = sqlSession.getConnection();
        Set<PreparedUpdates> prepared = new TreeSet<>((e1, e2) ->
                (e2.one2Many ? 1 : -1) * (e1.entity.parent == e2.entity ? -1 : (e2.entity.parent == e1.entity ? 1 : (
                        e2.entity.name.compareTo(e1.entity.name)
                ))));
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
                                    int lastDelim = entityPropertyPath.lastIndexOf('.');
                                    String parentPath = lastDelim < 0 ? null : entityPropertyPath.substring(0, lastDelim);
                                    if (!updatedEntities.containsKey(parentPath))
                                        updatedParents.computeIfAbsent(parentPath, k -> new HashSet<>())
                                                .add(getEntityForPath(entityPropertyPath).isCollection() ? updatedIndex : 0);
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
                    RowUpdates rowUpdates = new RowUpdates(row, dateNow);
                    TreeMap<String, Set<Integer>> updatedEntities = updates.get(row);
                    HashMap<String, Set<Integer>> updatedParents = new HashMap<>();
                    boolean isNewObject = "yes".equals(row.getColumnValue(0, "dDataAppendRowInTable_"));
                    CopyOnWriteArrayList<String> orderedPaths = new CopyOnWriteArrayList<>(updatedEntities.keySet());
                    for (String entityPropertyPath : orderedPaths) {
                        AbstractDataView.TableEntity entity = this.getEntityForPath(
                                entityPropertyPath.length()==0 ? null : entityPropertyPath
                        );
                        @SuppressWarnings("unchecked")
                        DDataBeanUpdateService beanService = getUpdateServiceFor(entity.name);
                        if (beanService == null || beanService.serviceDoesNotMakeUpdates()) {
                            PreparedUpdates pk = prepared.stream()
                                    .filter(k -> Objects.equals(entityPropertyPath, k.entityPropertyPath))
                                    .findAny().orElse(null);
                            if (pk == null)
                                prepared.add(pk = new PreparedUpdates(this, entity, entityPropertyPath, connection));

                            for (Integer updatedIndex : updatedEntities.get(entity.name)) {
                                String firstIdProp = pk.getFirstIdColumnName();
                                Object anyId = row.getColumnValue(updatedIndex,
                                        entity.name.length() == 0 ? firstIdProp :
                                                entity.name + "." + firstIdProp);

                                Map<AbstractDataView.TableEntity, AbstractDataView.TableCell> mapFromParent =
                                        entity.parent == null ? null : entity.parent.mappings.entrySet().stream()
                                                .filter(parentMap -> !parentMap.getKey().attribute.isPrimaryKey() &&
                                                        parentMap.getValue().get(entity) != null)
                                                .map(Map.Entry::getValue).findAny().orElse(null);
                                pk.one2Many = mapFromParent != null && mapFromParent.values().stream()
                                        .noneMatch(o -> o.attribute.isCollection());

                                if (isNewObject || idIsNull(anyId)) { //is new element
                                    if (beanService == null) {
                                        for (TableCell cell : entity.cells)
                                            if (cell.attribute.isPrimaryKey() && !cell.isVersion)
                                                fillIdAttribute(connection, row,
                                                        entity.beanInterface,
                                                        entity.name,
                                                        updatedIndex, cell, dateNow);
                                        pk.fillMappings(row, updatedIndex);
                                    }

                                    if (pk.one2Many)
                                        rowUpdates.addInsertBefore(pk, updatedIndex);
                                    else
                                        rowUpdates.addInsertAfter(pk, updatedIndex);
                                } else
                                    rowUpdates.addUpdate(pk, updatedIndex);

                                // if children inserts modify parent mapping attributes, update parents entities
                                if (pk.one2Many) {
                                    int lii = entity.name.lastIndexOf('.');
                                    String parentPath = lii < 0 ? null : entity.name.substring(0, lii);
                                    TableEntity parentEntity = getEntityForPath(parentPath);
                                    int parentIndex = entity.isCollection() ? 0 : updatedIndex;
                                    Set<Integer> uev = updatedEntities.get(parentEntity.name);
                                    if (uev == null) {
                                        updatedEntities.put(parentEntity.name, uev = new HashSet<>());
                                        orderedPaths.add(parentEntity.name);
                                    }
                                    uev.add(parentIndex);
                                }
                            }
                        }
                    }
                    rowUpdates.fillStatements();
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

    /**
     * Получает значение id-поля либо в соответсвии с аннотацией GeneratedValue, либо по связям из родительской сущности.
     *
     * @param connection         соединение с БД (для автогенерации)
     * @param row                строка таблицы для заполнения значения
     * @param beanInterface      интерфейс сущности (для получения аннотации)
     * @param entityPropertyPath путь до сущности (в имени столбца таблицы)
     * @param index              индекс значения столбца (для связей один ко многим)
     * @param cell               описание колонки таблицы
     * @param dateNow            текущая дата проведения изменений (для заполнения поля версии)
     * @throws NoSuchMethodException
     * @throws SQLException
     */
    private void fillIdAttribute(
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
            if (cell.attribute.equals(entity.versionFrom) && (
                    Temporal.class.isAssignableFrom(cell.attribute.getJavaType()) ||
                            Date.class.isAssignableFrom(cell.attribute.getJavaType())))
                row.setColumnValue(dateNow, index, idPropertyPath, false);
            else if (entity.parent != null) {
                Map.Entry<TableCell, TableCell> mapEntry = entity.parent.mappings.entrySet().stream()
                        .filter(m -> m.getValue().get(entity) != null)
                        .flatMap(m -> Collections.singletonMap(m.getKey(), m.getValue().get(entity)).entrySet().stream())
                        .filter(v -> v.getValue().name.equals(idPropertyPath))
                        .findAny().orElse(null);
                if (mapEntry != null) {
                    Object idVal = row.getColumnValue(entity.isCollection() ? 0 : index, mapEntry.getKey().name);
                    if (!idIsNull(idVal)) row.setColumnValue(idVal, index, idPropertyPath);
                }
            }
        } else {
            Object idValue = row.getColumnValue(index, idPropertyPath);
            if (idIsNull(idValue)) {
                String genValue = gen.value().length() == 0 ? gen.generator() : gen.value();
                switch (gen.strategy()) {
                    case SEQUENCE:
                        try (PreparedStatement st = connection.prepareStatement("SELECT nextval(?);")) {
                            st.setString(1, genValue);
                            try (ResultSet rs = st.executeQuery()) {
                                if (rs.next()) idValue = idFromResult(rs, cell.attribute);
                            }
                        }
                        break;
                    case SELECT:
                        try (Statement st = connection.createStatement()) {
                            try (ResultSet rs = st.executeQuery(genValue)) {
                                if (rs.next()) idValue = idFromResult(rs, cell.attribute);
                            }
                        }
                        break;
                }
                if (idValue != null)
                    row.setColumnValue(idValue, index, idPropertyPath, false);
            }
        }

        if (idIsNull(row.getColumnValue(index, idPropertyPath)))
            throw new RuntimeException("can't set property " + idPropertyPath);
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
        DDataBeanUpdateService<T> updater = (DDataBeanUpdateService<T>) knownUpdaters.get(beanInterface);
        if (updater == null)
            updater = Arrays.stream(beanInterface.getInterfaces())
                    .map(i -> knownUpdaters.get(i))
                    .filter(Objects::nonNull).findFirst()
                    .orElse(null);
        return updater;
    }

    public <T extends Serializable> void addUpdateService(Class<T> i, DDataBeanUpdateService<T> service) {
        knownUpdaters.put(i, service);
    }

    private class RowUpdates {
        final DDataViewRow row;
        final Date dateNow;
        final List<PreparedUpdates> insertBeforeStatements = new ArrayList<>();
        final List<Integer> insertBeforeIndexes = new ArrayList<>();
        final List<PreparedUpdates> updatesStatements = new ArrayList<>();
        final List<Integer> updatesIndexes = new ArrayList<>();
        final List<PreparedUpdates> insertAfterStatements = new ArrayList<>();
        final List<Integer> insertAfterIndexes = new ArrayList<>();

        RowUpdates(DDataViewRow row, Date dateNow) {
            this.row = row;
            this.dateNow = dateNow;
        }

        void addInsertBefore(PreparedUpdates pk, Integer updatedIndex) {
            insertBeforeStatements.add(pk);
            insertBeforeIndexes.add(updatedIndex);
        }

        void addInsertAfter(PreparedUpdates pk, Integer updatedIndex) {
            insertAfterStatements.add(pk);
            insertAfterIndexes.add(updatedIndex);
        }

        void addUpdate(PreparedUpdates pk, Integer updatedIndex) {
            updatesStatements.add(pk);
            updatesIndexes.add(updatedIndex);
        }

        void fillStatements() throws SQLException {
            for (int i = 0; i < insertBeforeStatements.size(); i++)
                insertBeforeStatements.get(i).fillInsert(row, insertBeforeIndexes.get(i), dateNow);
            for (int i = 0; i < updatesStatements.size(); i++)
                updatesStatements.get(i).fillUpdate(row, updatesIndexes.get(i), dateNow);
            for (int i = 0; i < insertAfterStatements.size(); i++)
                insertAfterStatements.get(i).fillInsert(row, insertAfterIndexes.get(i), dateNow);
        }
    }
}
