package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public class OracleDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") final String sql, long offset, long limit) {
        return "SELECT * FROM ( SELECT tmp.* , ROWNUM ROW_ID FROM ( " + sql + " ) tmp WHERE ROWNUM <= " + (offset + limit) + ") WHERE ROW_ID > " + offset;
    }

    @Override
    public String columnSql(String tableName) {
        return "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' ORDER BY COLUMN_ID";
    }

    @Override
    public String downRecursiveSql(String tableName, String startColumn, String joinColumn) {
        return "SELECT * FROM " + tableName + " START WITH " + startColumn + " = ? CONNECT BY PRIOR " + startColumn + " = " + joinColumn;
    }

    @Override
    public String upRecursiveSql(String tableName, String startColumn, String joinColumn) {
        return "SELECT * FROM " + tableName + " START WITH " + startColumn + " = ? CONNECT BY PRIOR " + joinColumn + " = " + startColumn;
    }
}