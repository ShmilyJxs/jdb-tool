package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public class PostgreSQLDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") String sql, long pageNum, long pageSize) {
        pageNum = Math.max(pageNum, 1L);
        long limit = Math.max(pageSize, 0L);
        long offset = (pageNum - 1L) * limit;
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
        return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
    }
}