package io.github.shmilyjxs.dialects;

import org.intellij.lang.annotations.Language;

public interface IDialect {

    String pageSql(@Language("SQL") final String sql, long offset, long limit);

    String columnSql(String tableName);

    String downRecursiveSql(String tableName, String startColumn, String joinColumn);

    String upRecursiveSql(String tableName, String startColumn, String joinColumn);
}