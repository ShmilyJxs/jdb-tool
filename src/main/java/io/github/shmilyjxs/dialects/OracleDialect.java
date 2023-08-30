package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public class OracleDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") String sql, long pageNum, long pageSize) {
        pageNum = Math.max(pageNum, 1L);
        long limit = Math.max(pageSize, 0L);
        long offset = (pageNum - 1L) * limit;
        return "SELECT * FROM ( SELECT tmp.* , ROWNUM ROW_ID FROM ( " + sql + " ) tmp WHERE ROWNUM <= " + (offset + limit) + ") WHERE ROW_ID > " + offset;
    }
}