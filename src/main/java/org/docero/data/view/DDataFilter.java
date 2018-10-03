package org.docero.data.view;

import org.docero.data.remote.DDataPrototypeRealization;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@SuppressWarnings({"unused", "WeakerAccess"})
public class DDataFilter {
    private final DDataAttribute attribute;
    private final DDataFilterOperator operator;
    private final Object value;
    private final Object valueTo;
    private final boolean externalData;

    private final List<DDataFilter> filters;

    private static final DDataAttribute ROOT_FILTER = new DDataAttribute() {
        @Override
        public String getColumnName() {
            return null;
        }

        @Override
        public String getPropertyName() {
            return null;
        }

        @Override
        public Class getJavaType() {
            return null;
        }

        @Override
        public String getJdbcType() {
            return null;
        }

        @Override
        public boolean isDictionary() {
            return false;
        }

        @Override
        public boolean isMappedBean() {
            return false;
        }

        @Override
        public boolean isCollection() {
            return false;
        }

        @Override
        public boolean isPrimaryKey() {
            return false;
        }

        @Override
        public String joinTable() {
            return null;
        }

        @Override
        public Map<String, String> joinMapping() {
            return null;
        }

        @Override
        public Class<? extends Serializable> getBeanInterface() {
            return null;
        }
    };
    private Boolean sortAscending;
    private String mapName;

    /**
     * Find attribute for property name in given attribute
     *
     * @param propertyName property name
     * @param in           attribute of mapped bean
     * @return null if not found
     */
    public static DDataAttribute find(String propertyName, DDataAttribute in) {
        if (in.getJavaType() != null)
            for (Field field : in.getJavaType().getDeclaredFields())
                if (field.isEnumConstant()) {
                    @SuppressWarnings("unchecked")
                    DDataAttribute atr = (DDataAttribute) Enum.valueOf((Class<? extends Enum>) in.getJavaType(), field.getName());
                    if (propertyName.equals(atr.getPropertyName())) {
                        return atr;
                    }
                }
        return null;
    }

    /**
     * Find attribute for table column name in given attribute
     *
     * @param columnName name of table column name
     * @param in         attribute of mapped bean
     * @return null if not found
     */
    public static DDataAttribute findByColumn(String columnName, DDataAttribute in) {
        if (in.getJavaType() != null)
            for (Field field : in.getJavaType().getDeclaredFields())
                if (field.isEnumConstant()) {
                    @SuppressWarnings("unchecked")
                    DDataAttribute atr = (DDataAttribute) Enum.valueOf((Class<? extends Enum>) in.getJavaType(), field.getName());
                    if (columnName.equalsIgnoreCase(atr.getColumnName())) {
                        return atr;
                    }
                }
        return null;
    }

    /**
     * Creating rootEntity element of data filters tree
     */
    public DDataFilter() {
        filters = new ArrayList<>();
        operator = null;
        value = null;
        externalData = false;
        valueTo = null;
        attribute = ROOT_FILTER;
    }

    /**
     * Create element in filters patch (tree branch)
     *
     * @param column attribute of mapped bean or collection of beans
     * @throws DDataException if DDataAttribute is null or NONE_
     */
    public DDataFilter(DDataAttribute column) throws DDataException {
        this(column, null, null, null);
    }

    /**
     * Create element in filters patch (tree branch) for aggregating view column
     *
     * @param column attribute of mapped bean or collection of beans
     * @throws DDataException if DDataAttribute is null or NONE_
     */
    public DDataFilter(DDataAttribute column, DDataFilterOperator operator) throws DDataException {
        this(column, operator, null, null);
    }

    /**
     * Create concrete filter (tree leaf) for value
     *
     * @param column   attribute to filter
     * @param operator filter operation
     * @param value    value
     * @throws DDataException if DDataAttribute is null or NONE_
     */
    public DDataFilter(DDataAttribute column, DDataFilterOperator operator, Object value) throws DDataException {
        this(column, operator, value, null);
    }

    /**
     * Create concrete filter (tree leaf) for range
     *
     * @param column   attribute to filter
     * @param operator filter operation
     * @param value    start of range value
     * @param valueTo  end of range value
     * @throws DDataException if DDataAttribute is null or NONE_
     */
    public DDataFilter(DDataAttribute column, DDataFilterOperator operator, Object value, Object valueTo) throws DDataException {
        if (column == null || column.getPropertyName() == null)
            throw new DDataException("can't create filter for NULL");

        this.attribute = column;
        if (attribute.isMappedBean()) {
            filters = new ArrayList<>();
            this.externalData = attribute.getBeanInterface().isAnnotationPresent(DDataPrototypeRealization.class);
            this.operator = operator;
            this.value = value;
            this.valueTo = valueTo;
        } else {
            filters = null;
            this.externalData = false;
            this.operator = operator;
            this.value = value;
            this.valueTo = valueTo;
        }
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public DDataFilter clone() {
        try {
            DDataFilter c = new DDataFilter(this.attribute, this.operator, this.value, this.valueTo);
            if (this.filters != null) {
                c.filters.addAll(this.filters.stream().map(DDataFilter::clone).collect(Collectors.toList()));
            }
            return c;
        } catch (DDataException e) {
            return null;
        }
    }

    /**
     * add child for filter element (branch of leaf)
     *
     * @param filter child filter
     * @return current filter element
     * @throws DDataException if DDataAttribute is null or NONE_
     */
    public DDataFilter add(DDataFilter filter) throws DDataException {
        if (filter == null) return this;

        if (this.filters != null)
            if (attribute == ROOT_FILTER) {
                this.filters.add(filter);
                return this;
            } else for (Field field : attribute.getJavaType().getDeclaredFields())
                if (field.isEnumConstant()) {
                    @SuppressWarnings("unchecked")
                    DDataAttribute atr = (DDataAttribute) Enum.valueOf((Class<? extends Enum>) attribute.getJavaType(), field.getName());
                    if (atr.getPropertyName() != null && atr.getPropertyName().equals(filter.attribute.getPropertyName())) {
                        this.filters.add(filter);
                        return this;
                    }
                }
        throw new DDataException("can't add filter by " + filter.attribute.getPropertyName() + " to " + attribute.getPropertyName());
    }

    public DDataAttribute getAttribute() {
        return attribute;
    }

    public boolean isExternalData() {
        return externalData;
    }

    public DDataFilterOperator getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    public Object getValueTo() {
        return valueTo;
    }

    public List<DDataFilter> getFilters() {
        return filters;
    }

    /**
     * getFilters not returns null if attribute is mapped bean
     *
     * @return is filter attribute is mapped bean
     */
    public boolean hasFilters() {
        return filters != null && !filters.isEmpty();
    }

    String getName() {
        return attribute == null ? "" : attribute.getPropertyName();
    }

    @Override
    public int hashCode() {
        return attribute.hashCode();
    }

    /**
     * Override method
     *
     * @param o compared object
     * @return true if attribute is equals
     */
    @Override
    public boolean equals(Object o) {
        return o != null && o instanceof DDataFilter &&
                Objects.equals(((DDataFilter) o).attribute, attribute);
    }

    public Boolean isSortAscending() {
        return sortAscending;
    }

    public void setSortAscending(Boolean sortAscending) {
        this.sortAscending = sortAscending;
    }

    public String getMapName() {
        return mapName != null ? mapName : (attribute == null ? "" : attribute.getPropertyName());
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }
}
