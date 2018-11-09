package org.docero.data.view;

import java.util.*;
import java.util.stream.Collectors;

public class DDataViewRows {
    private final DDataView view;
    private final List<Map<String, Object>> map;

    DDataViewRows(DDataView view, List<Map<String, Object>> map) {
        this.view = view;
        this.map = map;
    }

    public List<Map<String, Object>> toList() {
        return map;
    }

    public List<Map<String, Object>> toListStrict() {
        return map.stream().map(e -> {
            DDataViewRow row = new DDataViewRow(view, e);
            HashMap<String, Object> v = new HashMap<>();
            v.put("dDataBeanKey_", e.get("dDataBeanKey_"));
            for (AbstractDataView.TableCell tableCell : view.tableCells.values())
                if (tableCell.column != null) {
                    Object[] colVal = row.getColumn(tableCell.name);
                    if (colVal != null && colVal.length > 0 && !(colVal.length == 1 && colVal[0] == null)) {
                        //String colName = tableCell.column.getMapName();
                        AbstractDataView.putInHierarchy(v, tableCell.name,
                                colVal.length == 1 ? colVal[0] : colVal);
                    }
                }
            return v;
        }).collect(Collectors.toList());
    }

    @SuppressWarnings("unused")
    public Set<Object> keySet() {
        return map.stream()
                .map(o -> o.get("dDataBeanKey_"))
                .collect(Collectors.toSet());
    }

    public DDataViewRow getRow(Object key) {
        return new DDataViewRow(view, map.stream()
                .filter(o -> Objects.equals(key, o.get("dDataBeanKey_")))
                .findAny().orElse(null));
    }

    public DDataViewRow getRow(int index) {
        return new DDataViewRow(view, map.get(index));
    }

    public int size() {
        return map.size();
    }
}
