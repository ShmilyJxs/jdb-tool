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
        return new StringBuilder("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '").append(tableName).append("'").toString();
    }
}