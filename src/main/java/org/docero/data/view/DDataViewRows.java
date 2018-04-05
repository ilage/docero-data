package org.docero.data.view;

import org.docero.data.DDataOrder;

import java.util.*;
import java.util.stream.Collectors;

public class DDataViewRows {
    private final DDataView view;
    private final Map<Object, Object> map;

    DDataViewRows(DDataView view, Map<Object, Object> map) {
        this.view = view;
        this.map = map;
    }

    @SuppressWarnings("unchecked")
    public List<Object> toList() {
        return map.values().stream().sorted((o1, o2) -> {
            for (DDataView.Sort sort : view.sortedPaths) {
                Object v1 = DDataViewRow.getColumnValue((Map<Object, Object>) o1, 0, sort.path);
                Object v2 = DDataViewRow.getColumnValue((Map<Object, Object>) o2, 0, sort.path);
                if (v1 == null) {
                    if (v2 != null) return sort.asc ? 1 : 0;
                } else if (v1 instanceof Comparable) {
                    int cmp = ((Comparable) v1).compareTo(v2);
                    if (cmp != 0) return sort.asc ? cmp : -cmp;
                }
            }
            return 0;
        }).collect(Collectors.toList());
    }

    public Set<Object> keySet() {
        return map.keySet();
    }

    public DDataViewRow getRow(Object key) {
        return new DDataViewRow(view, map.get(key));
    }

    private List<Object> orderedView;

    public void setOrder(DDataOrder order) {
        orderedView = new ArrayList<>(map.keySet());
    }

    public DDataViewRow getRow(int index) {
        return orderedView == null ? null : new DDataViewRow(view, map.get(orderedView.get(index)));
    }

    public int size() {
        return map.size();
    }
}
