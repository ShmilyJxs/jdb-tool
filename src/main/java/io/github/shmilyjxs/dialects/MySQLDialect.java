package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public class MySQLDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") String sql, long offset, long limit) {
        StringBuilder stringBuilder = new StringBuilder(sql);
        stringBuilder.append(" LIMIT ");
        if (offset == 0L) {
            stringBuilder.append(limit);
        } else {
            stringBuilder.append(offset).append(" , ").append(limit);
        }
        return stringBuilder.toString();
    }

    @Override
    public String columnSql(String tableName) {
        return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
    }
}