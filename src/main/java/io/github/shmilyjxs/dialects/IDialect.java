package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public interface IDialect {

    String pageSql(@Language("SQL") String sql, long offset, long limit);

    String columnSql(String tableName);
}