package org.docero.data.view;

import org.docero.data.utils.DDataAttribute;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public class DDataViewRow {
    final DDataView view;
    private final Map<Object, Object> map;

    @SuppressWarnings("unchecked")
    public DDataViewRow(DDataView view, Object o) {
        this.view = view;
        if (o instanceof Map) map = (Map<Object, Object>) o;
        else map = null;
    }

    public Object[] getColumn(DDataAttribute... path) {
        return getColumn(Arrays.stream(path)
                .map(DDataAttribute::getPropertyName)
                .collect(Collectors.joining(".")));
    }

    public Object[] getColumn(String path) {
        if (map == null) return null;
        return columnValue(map, path.split("\\."), 0);
    }

    public int getColumnValueIndex(Object value, DDataAttribute... path) {
        return getColumnValueIndex(value, Arrays.stream(path)
                .map(DDataAttribute::getPropertyName)
                .collect(Collectors.joining(".")));
    }

    public int getColumnValueIndex(Object value, String path) {
        Object[] a = getColumn(path);
        for (int i = 0; i < a.length; i++) if (Objects.equals(a[i], value)) return i;
        return -1;
    }

    /**
     * Returns value in column values by given index, if index is out of range returns null.
     *
     * @param index index of value in column (if not collection use 0)
     * @param path  array of property attributes
     * @return value in column
     */
    public Object getColumnValue(int index, DDataAttribute... path) {
        return getColumnValue(index, Arrays.stream(path)
                .map(DDataAttribute::getPropertyName)
                .collect(Collectors.joining(".")));
    }

    /**
     * Returns value in column values by given index, if index is out of range returns null.
     *
     * @param index index of value in column (if not collection use 0)
     * @param path  array of property names
     * @return value in column
     */
    public Object getColumnValue(int index, String path) {
        if (map == null) return null;
        int idx = path.lastIndexOf('.');
        String parName = idx < 0 ? path : path.substring(idx + 1);
        String parPath = idx < 0 ? "" : path.substring(0, idx + 1);
        DDataFilter column = view.viewPaths.entrySet().stream()
                .filter(pair ->
                        pair.getValue().startsWith(parPath) &&
                                pair.getKey().getAttribute().getPropertyName().equals(parName))
                .map(Map.Entry::getKey).findAny().orElse(null);
        String[] aPath = path.split("\\.");
        if (column != null && column.mapToName() != null)
            aPath[aPath.length - 1] = column.mapToName();
        Object[] a = columnValue(map, aPath, 0);
        if (a.length < index) a = getColumn(path);
        return a.length <= index || index < 0 ? null : a[index];
    }

    static Object getColumnValue(Map<Object, Object> map, int index, String path) {
        if (map == null) return null;
        Object[] a = columnValue(map, path.split("\\."), 0);
        return a.length <= index || index < 0 ? null : a[index];
    }

    @SuppressWarnings("unchecked")
    private static Object[] columnValue(Map<Object, Object> om, String[] path, int offset) {
        if (offset >= path.length) return new Object[]{om};
        Object o = om.get(path[offset]);
        if (o == null) return new Object[]{null};

        if (o instanceof Map) {
            return columnValue((Map<Object, Object>) o, path, offset + 1);
        } else if (o instanceof List) {
            Object[] ret = new Object[((List) o).size()];
            for (int i = 0; i < ret.length; i++) {
                Object lv = ((List) o).get(i);
                if (lv instanceof Map)
                    ret[i] = columnValue((Map<Object, Object>) lv, path, offset + 1)[0];
                else
                    ret[i] = lv;
            }
            return ret;
        } else if (offset == path.length - 1)
            return new Object[]{o};
        else
            return new Object[]{null};
    }

    /**
     * Update value of row column. Make it in-place in the row data map, and
     * store update in parent DDataView for serialize to database by flushUpdates() method.
     *
     * @param value new value
     * @param index index of value in column values
     * @param path  entityPropertyPath to column
     */
    public void setColumnValue(Object value, int index, DDataAttribute... path) {
        setColumnValue(value, index, Arrays.stream(path)
                .map(DDataAttribute::getPropertyName)
                .collect(Collectors.joining(".")));
    }

    /**
     * Update value of row column. Make it in-place in the row data map, and
     * store update in parent DDataView for serialize to database by flushUpdates() method.
     *
     * @param value          new value
     * @param index          index of value in column values
     * @param path2Parameter entityPropertyPath to column
     */
    @SuppressWarnings("unchecked")
    public void setColumnValue(Object value, int index, String path2Parameter) {
        setColumnValue(value, index, path2Parameter, true);
    }

    void setColumnValue(Object value, int index, String path2Parameter, boolean addViewUpdate) {
        if (map == null) return;

        Map<Object, Object> innerMap = map;
        String[] path = path2Parameter.split("\\.");
        int currPathLen = 0;
        for (int offset = 0; offset < path.length; offset++) {
            boolean lastElement = (offset == path.length - 1);
            currPathLen = currPathLen + path[offset].length() + (lastElement ? 0 : 1);
            DDataAttribute attribute = view.getEntityForPath(path2Parameter.substring(0, currPathLen));

            Object o = innerMap.get(path[offset]);

            if (o == null) {
                if (lastElement) innerMap.put(path[offset], value);
                else if (attribute != null && attribute.isCollection()) {
                    HashMap<Object, Object> innerInList = new HashMap<>();
                    innerMap.put(path[offset], new ArrayList<Object>() {{
                        for (int i = 0; i < index; i++) this.add(null);
                        this.add(innerInList);
                    }});
                    innerMap = innerInList;
                } else innerMap.put(path[offset], innerMap = new HashMap<>());
            } else if (o instanceof Map) innerMap = (Map<Object, Object>) o;
            else if (o instanceof List) {
                if (((List) o).size() > index) {
                    Object lv = ((List) o).get(index);
                    if (lv instanceof Map) innerMap = (Map<Object, Object>) lv;
                    else if (lastElement) ((List) o).set(index, value);
                    else ((List) o).set(index, (innerMap = new HashMap<>()));
                } else {
                    while (((List) o).size() < index) ((List) o).add(null);
                    ((List) o).add(lastElement ? value : (innerMap = new HashMap<>()));
                }
            } else if (lastElement)
                innerMap.put(path[offset], value);
        }

        if(addViewUpdate) view.addUpdate(this, index, path2Parameter);
    }
}
