package org.docero.data.view;

import org.docero.data.DDataOrder;

import java.util.*;

public class DDataViewRows {
    private final DDataView view;
    private final Map<Object, Object> map;

    DDataViewRows(DDataView view, Map<Object, Object> map) {
        this.view = view;
        this.map = map;
    }

    public List<Object> toList() {
        return new ArrayList<>(map.values());
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
