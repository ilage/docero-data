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
