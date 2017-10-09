package org.docero.data;

import org.docero.data.utils.DDataAttribute;

import java.util.ArrayList;
import java.util.List;

public class DDataOrder<T extends DDataAttribute> {
    static class OrderEntry {
        private final DDataAttribute attribute;
        private final boolean ascending;

        private OrderEntry(DDataAttribute attribute, boolean ascending) {
            this.attribute = attribute;
            this.ascending = ascending;
        }

        public DDataAttribute getAttribute() {
            return attribute;
        }

        public boolean isAscending() {
            return ascending;
        }

        public String getOrder() {
            return ascending ? "ASC" : "DESC";
        }
    }

    private final ArrayList<OrderEntry> order = new ArrayList<>();

    public static <T extends DDataAttribute> DDataOrder<T> asc(T attribute) {
        DDataOrder<T> dor = new DDataOrder<T>();
        dor.order.add(new OrderEntry(attribute, true));
        return dor;
    }

    public static <T extends DDataAttribute> DDataOrder<T> desc(T attribute) {
        DDataOrder<T> dor = new DDataOrder<T>();
        dor.order.add(new OrderEntry(attribute, false));
        return dor;
    }

    public DDataOrder<T> addAsc(T attribute) {
        this.order.add(new OrderEntry(attribute, true));
        return this;
    }

    public DDataOrder<T> addDesc(T attribute) {
        this.order.add(new OrderEntry(attribute, false));
        return this;
    }

    public List<OrderEntry> getOrder() {
        return order;
    }
}
