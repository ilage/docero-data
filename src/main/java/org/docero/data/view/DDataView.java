package org.docero.data.view;

import org.apache.ibatis.jdbc.SQL;
import org.docero.data.utils.DDataAttribute;
import org.docero.data.utils.DDataException;

import java.lang.reflect.Field;
import java.time.temporal.Temporal;
import java.util.*;

@SuppressWarnings("unused")
public class DDataView extends AbstractDataView {
    private final Class[] roots;
    private final DDataFilter[] columns;
    private DDataFilter filter = new DDataFilter();
    private final Temporal version;

    Temporal version() {
        return version;
    }

    public DDataView(List<Class<? extends DDataAttribute>> alternativeRoots, List<DDataFilter> columns) {
        this.roots = alternativeRoots.toArray(new Class[alternativeRoots.size()]);
        this.columns = columns.toArray(new DDataFilter[columns.size()]);
        this.version = null;
    }

    public DDataView(Class<? extends DDataAttribute> root, List<DDataFilter> columns) {
        this.roots = new Class[]{root};
        this.columns = columns.toArray(new DDataFilter[columns.size()]);
        this.version = null;
    }

    public DDataView(Class<? extends DDataAttribute> root, DDataFilter... columns) {
        this.roots = new Class[]{root};
        this.columns = columns;
        this.version = null;
    }

    public DDataView(List<Class<? extends DDataAttribute>> alternativeRoots, Temporal version, List<DDataFilter> columns) {
        this.roots = alternativeRoots.toArray(new Class[alternativeRoots.size()]);
        this.columns = columns.toArray(new DDataFilter[columns.size()]);
        this.version = version;
    }

    public DDataView(Class<? extends DDataAttribute> root, Temporal version, List<DDataFilter> columns) {
        this.roots = new Class[]{root};
        this.columns = columns.toArray(new DDataFilter[columns.size()]);
        this.version = version;
    }

    public DDataView(Class<? extends DDataAttribute> root, Temporal version, DDataFilter... columns) {
        this.roots = new Class[]{root};
        this.columns = columns;
        this.version = version;
    }

    public DDataFilter getFilter() {
        return filter;
    }

    public void setFilter(DDataFilter filter) {
        this.filter = filter;
    }

    public SQL count() throws DDataException {
        SQL sql = new SQL() {{
            SELECT("COUNT(*)");
        }};
        try {
            sql.FROM((String) roots[0].getDeclaredField("TABLE_NAME").get(null) + " as t0");
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new DDataException("select view not from *_WB_ enum: " + roots[0].getCanonicalName());
        }
        buildFilters(sql, new HashMap<String, Integer>() {{
            put("", 0); // empty path is FROM table
        }});
        return sql;
    }

    public SQL select() throws DDataException {
        HashMap<String, Integer> usedCols = new HashMap<String, Integer>() {{
            put("", 0); // empty path is FROM table
        }};
        SQL sql = new SQL();
        for (DDataFilter column : columns)
            for (Class root : roots)
                if (super.isApplicable(root, column)) {
                    super.addColumnToViewSql(sql, usedCols, root, column, "", 0);
                    break;
                }

        try {
            sql.FROM((String) roots[0].getDeclaredField("TABLE_NAME").get(null) + " as t0");
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new DDataException("select view not from *_WB_ enum: " + roots[0].getCanonicalName());
        }
        buildFilters(sql, usedCols);
        return sql;
    }

    private void buildFilters(SQL sql, Map<String, Integer> usedCols) throws DDataException {
        for (int i = 0; i < roots.length; i++) {
            if (i > 0) sql.OR();
            try {
                Field discriminator = roots[i].getDeclaredField("DISCR_ATTR");
                Field dval = roots[i].getDeclaredField("DISCR_VAL");
                DDataAttribute discriminatorAttribute = (DDataAttribute) discriminator.get(null);
                if (discriminatorAttribute != null)
                    sql.WHERE("t0." + discriminatorAttribute.getColumnName() + " = " + dval.get(null));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new DDataException("select view not from *_WB_ enum: " + roots[i].getCanonicalName());
            }
            String verSql = versionalConstraint(roots[i], 0);
            if (verSql.length() > 0) sql.WHERE(verSql);
            super.addFilterSql(sql, filter.getFilters(), usedCols, "", 0);
        }
    }
}
