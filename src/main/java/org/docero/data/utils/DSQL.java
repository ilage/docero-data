package org.docero.data.utils;

import org.apache.ibatis.jdbc.AbstractSQL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DSQL {
    private static final String AND = ") \nAND (";
    private static final String OR = ") \nOR (";

    private final SQLStatement sql = new SQLStatement();

    public DSQL UPDATE(String table) {
        sql.statementType = SQLStatement.StatementType.UPDATE;
        sql.tables.add(table);
        return this;
    }

    public DSQL SET(String sets) {
        sql.sets.add(sets);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL SET(String... sets) {
        sql.sets.addAll(Arrays.asList(sets));
        return this;
    }

    public DSQL INSERT_INTO(String tableName) {
        sql.statementType = SQLStatement.StatementType.INSERT;
        sql.tables.add(tableName);
        return this;
    }

    public DSQL VALUES(String columns, String values) {
        sql.columns.add(columns);
        sql.values.add(values);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL INTO_COLUMNS(String... columns) {
        sql.columns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL INTO_VALUES(String... values) {
        sql.values.addAll(Arrays.asList(values));
        return this;
    }

    public DSQL SELECT(String columns) {
        sql.statementType = SQLStatement.StatementType.SELECT;
        sql.select.add(columns);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL SELECT(String... columns) {
        sql.statementType = SQLStatement.StatementType.SELECT;
        sql.select.addAll(Arrays.asList(columns));
        return this;
    }

    public DSQL SELECT_DISTINCT(String columns) {
        sql.distinct = true;
        SELECT(columns);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL SELECT_DISTINCT(String... columns) {
        sql.distinct = true;
        SELECT(columns);
        return this;
    }

    public DSQL DELETE_FROM(String table) {
        sql.statementType = SQLStatement.StatementType.DELETE;
        sql.tables.add(table);
        return this;
    }

    public DSQL FROM(String table) {
        sql.tables.add(table);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL FROM(String... tables) {
        sql.tables.addAll(Arrays.asList(tables));
        return this;
    }

    public DSQL JOIN(String join) {
        sql.join.add(join);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL JOIN(String... joins) {
        sql.join.addAll(Arrays.asList(joins));
        return this;
    }

    public DSQL INNER_JOIN(String join) {
        sql.innerJoin.add(join);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL INNER_JOIN(String... joins) {
        sql.innerJoin.addAll(Arrays.asList(joins));
        return this;
    }

    public DSQL LEFT_OUTER_JOIN(String join) {
        sql.leftOuterJoin.add(join);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL LEFT_OUTER_JOIN(String... joins) {
        sql.leftOuterJoin.addAll(Arrays.asList(joins));
        return this;
    }

    public DSQL RIGHT_OUTER_JOIN(String join) {
        sql.rightOuterJoin.add(join);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL RIGHT_OUTER_JOIN(String... joins) {
        sql.rightOuterJoin.addAll(Arrays.asList(joins));
        return this;
    }

    public DSQL OUTER_JOIN(String join) {
        sql.outerJoin.add(join);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL OUTER_JOIN(String... joins) {
        sql.outerJoin.addAll(Arrays.asList(joins));
        return this;
    }

    public DSQL WHERE(String conditions) {
        sql.where.add(conditions);
        sql.lastList = sql.where;
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL WHERE(String... conditions) {
        sql.where.addAll(Arrays.asList(conditions));
        sql.lastList = sql.where;
        return this;
    }

    /**
     *  DData addon: add where part and all joins from parameter
     */
    public DSQL WHERE(DSQL w) {
        if(w.sql.where.size()>0) {
            StringBuilder builder = new StringBuilder();
            sql.join.addAll(w.sql.join);
            sql.innerJoin.addAll(w.sql.innerJoin);
            sql.outerJoin.addAll(w.sql.outerJoin);
            sql.leftOuterJoin.addAll(w.sql.leftOuterJoin);
            sql.rightOuterJoin.addAll(w.sql.rightOuterJoin);
            sql.sqlClause(new SafeAppendable(builder), "", w.sql.where, "(", ")", " AND ");
            return WHERE("("+builder.toString()+")");
        }
        else
            return this;
    }

    public DSQL OR() {
        sql.lastList.add(OR);
        return this;
    }

    public DSQL AND() {
        sql.lastList.add(AND);
        return this;
    }

    public DSQL GROUP_BY(String columns) {
        sql.groupBy.add(columns);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL GROUP_BY(String... columns) {
        sql.groupBy.addAll(Arrays.asList(columns));
        return this;
    }

    public DSQL HAVING(String conditions) {
        sql.having.add(conditions);
        sql.lastList = sql.having;
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL HAVING(String... conditions) {
        sql.having.addAll(Arrays.asList(conditions));
        sql.lastList = sql.having;
        return this;
    }

    public DSQL ORDER_BY(String columns) {
        sql.orderBy.add(columns);
        return this;
    }

    /**
     * @since 3.4.2
     */
    public DSQL ORDER_BY(String... columns) {
        sql.orderBy.addAll(Arrays.asList(columns));
        return this;
    }

    public <A extends Appendable> A usingAppender(A a) {
        sql.sql(a);
        return a;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sql.sql(sb);
        return sb.toString();
    }

    private static class SafeAppendable {
        private final Appendable a;
        private boolean empty = true;

        public SafeAppendable(Appendable a) {
            super();
            this.a = a;
        }

        public SafeAppendable append(CharSequence s) {
            try {
                if (empty && s.length() > 0) {
                    empty = false;
                }
                a.append(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public boolean isEmpty() {
            return empty;
        }

    }

    private static class SQLStatement {

        public enum StatementType {
            DELETE, INSERT, SELECT, UPDATE
        }

        SQLStatement.StatementType statementType;
        List<String> sets = new ArrayList<String>();
        List<String> select = new ArrayList<String>();
        List<String> tables = new ArrayList<String>();
        List<String> join = new ArrayList<String>();
        List<String> innerJoin = new ArrayList<String>();
        List<String> outerJoin = new ArrayList<String>();
        List<String> leftOuterJoin = new ArrayList<String>();
        List<String> rightOuterJoin = new ArrayList<String>();
        List<String> where = new ArrayList<String>();
        List<String> having = new ArrayList<String>();
        List<String> groupBy = new ArrayList<String>();
        List<String> orderBy = new ArrayList<String>();
        List<String> lastList = new ArrayList<String>();
        List<String> columns = new ArrayList<String>();
        List<String> values = new ArrayList<String>();
        boolean distinct;

        public SQLStatement() {
            // Prevent Synthetic Access
        }

        private void sqlClause(SafeAppendable builder, String keyword, List<String> parts, String open, String close,
                               String conjunction) {
            if (!parts.isEmpty()) {
                if (!builder.isEmpty()) {
                    builder.append("\n");
                }
                builder.append(keyword);
                builder.append(" ");
                builder.append(open);
                String last = "________";
                for (int i = 0, n = parts.size(); i < n; i++) {
                    String part = parts.get(i);
                    if (i > 0 && !part.equals(AND) && !part.equals(OR) && !last.equals(AND) && !last.equals(OR)) {
                        builder.append(conjunction);
                    }
                    builder.append(part);
                    last = part;
                }
                builder.append(close);
            }
        }

        private String selectSQL(SafeAppendable builder) {
            if (distinct) {
                sqlClause(builder, "SELECT DISTINCT", select, "", "", ", ");
            } else {
                sqlClause(builder, "SELECT", select, "", "", ", ");
            }

            sqlClause(builder, "FROM", tables, "", "", ", ");
            joins(builder);
            sqlClause(builder, "WHERE", where, "(", ")", " AND ");
            sqlClause(builder, "GROUP BY", groupBy, "", "", ", ");
            sqlClause(builder, "HAVING", having, "(", ")", " AND ");
            sqlClause(builder, "ORDER BY", orderBy, "", "", ", ");
            return builder.toString();
        }

        private void joins(SafeAppendable builder) {
            sqlClause(builder, "JOIN", join, "", "", "\nJOIN ");
            sqlClause(builder, "INNER JOIN", innerJoin, "", "", "\nINNER JOIN ");
            sqlClause(builder, "OUTER JOIN", outerJoin, "", "", "\nOUTER JOIN ");
            sqlClause(builder, "LEFT OUTER JOIN", leftOuterJoin, "", "", "\nLEFT OUTER JOIN ");
            sqlClause(builder, "RIGHT OUTER JOIN", rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ");
        }

        private String insertSQL(SafeAppendable builder) {
            sqlClause(builder, "INSERT INTO", tables, "", "", "");
            sqlClause(builder, "", columns, "(", ")", ", ");
            sqlClause(builder, "VALUES", values, "(", ")", ", ");
            return builder.toString();
        }

        private String deleteSQL(SafeAppendable builder) {
            sqlClause(builder, "DELETE FROM", tables, "", "", "");
            sqlClause(builder, "WHERE", where, "(", ")", " AND ");
            return builder.toString();
        }

        private String updateSQL(SafeAppendable builder) {
            sqlClause(builder, "UPDATE", tables, "", "", "");
            joins(builder);
            sqlClause(builder, "SET", sets, "", "", ", ");
            sqlClause(builder, "WHERE", where, "(", ")", " AND ");
            return builder.toString();
        }

        public String sql(Appendable a) {
            SafeAppendable builder = new SafeAppendable(a);
            if (statementType == null) {
                return null;
            }

            String answer;

            switch (statementType) {
                case DELETE:
                    answer = deleteSQL(builder);
                    break;

                case INSERT:
                    answer = insertSQL(builder);
                    break;

                case SELECT:
                    answer = selectSQL(builder);
                    break;

                case UPDATE:
                    answer = updateSQL(builder);
                    break;

                default:
                    answer = null;
            }

            return answer;
        }
    }
}
