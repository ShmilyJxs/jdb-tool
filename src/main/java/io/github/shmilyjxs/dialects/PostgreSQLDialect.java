package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public class PostgreSQLDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") final String sql, long offset, long limit) {
        StringBuilder stringBuilder = new StringBuilder(sql);
        stringBuilder.append(" LIMIT ");
        stringBuilder.append(limit);
        if (offset > 0L) {
            stringBuilder.append(" OFFSET ").append(offset);
        }
        return stringBuilder.toString();
    }

    @Override
    public String columnSql(String tableName) {
        return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' ORDER BY ORDINAL_POSITION";
    }

    @Override
    public String downRecursiveSql(String tableName, String startColumn, String joinColumn) {
        return "WITH RECURSIVE cte AS ( SELECT * FROM " + tableName + " WHERE " + startColumn + " = ? UNION ALL SELECT deep.* FROM " + tableName + " deep JOIN cte ON deep." + joinColumn + " = cte." + startColumn + " ) SELECT * FROM cte";
    }

    @Override
    public String upRecursiveSql(String tableName, String startColumn, String joinColumn) {
        return "WITH RECURSIVE cte AS ( SELECT * FROM " + tableName + " WHERE " + startColumn + " = ? UNION ALL SELECT deep.* FROM " + tableName + " deep JOIN cte ON deep." + startColumn + " = cte." + joinColumn + " ) SELECT * FROM cte";
    }
}