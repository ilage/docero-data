package org.docero.data;

import org.docero.data.utils.DDataBasicAttribute;

import java.util.ArrayList;
import java.util.List;

public class DDataOrder<T extends DDataBasicAttribute> {
    public static class OrderEntry {
        private final DDataBasicAttribute attribute;
        private final boolean ascending;

        private OrderEntry(DDataBasicAttribute attribute, boolean ascending) {
            this.attribute = attribute;
            this.ascending = ascending;
        }

        public DDataBasicAttribute getAttribute() {
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

    public static <T extends DDataBasicAttribute> DDataOrder<T> asc(T attribute) {
        DDataOrder<T> dor = new DDataOrder<>();
        dor.order.add(new OrderEntry(attribute, true));
        return dor;
    }

    public static <T extends DDataBasicAttribute> DDataOrder<T> desc(T attribute) {
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
