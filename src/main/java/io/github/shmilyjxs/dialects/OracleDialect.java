package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public class OracleDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") final String sql, long offset, long limit) {
        return new StringBuilder("SELECT * FROM ( SELECT tmp.* , ROWNUM ROW_ID FROM ( ").append(sql).append(" ) tmp WHERE ROWNUM <= ").append(offset + limit).append(") WHERE ROW_ID > ").append(offset).toString();
    }

    @Override
    public String columnSql(String tableName) {
        return new StringBuilder("SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '").append(tableName.toUpperCase()).append("'").toString();
    }
}