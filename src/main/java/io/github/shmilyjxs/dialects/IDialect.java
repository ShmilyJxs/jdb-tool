package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public interface IDialect {

    String pageSql(@Language("SQL") String sql, long pageNum, long pageSize);

    String columnSql(String tableName);
}