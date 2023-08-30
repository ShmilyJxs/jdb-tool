package com.github.dialects;

import org.intellij.lang.annotations.Language;

public class MySQLDialect implements IDialect {

    @Override
    public String pageSql(@Language("SQL") String sql, long pageNum, long pageSize) {
        pageNum = Math.max(pageNum, 1L);
        long limit = Math.max(pageSize, 0L);
        long offset = (pageNum - 1L) * limit;
        StringBuilder stringBuilder = new StringBuilder(sql);
        stringBuilder.append(" LIMIT ");
        if (offset == 0L) {
            stringBuilder.append(limit);
        } else {
            stringBuilder.append(offset).append(" , ").append(limit);
        }
        return stringBuilder.toString();
    }
}