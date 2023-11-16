package io.github.shmilyjxs.core;

import io.github.shmilyjxs.utils.PageResult;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;

public interface INativeDao {

    <T> T scalar(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    <T> List<T> scalarList(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    long count(@Language("SQL") final String sql, Object... args);

    boolean exists(@Language("SQL") final String sql, Object... args);

    int nativeUpdate(@Language("SQL") final String sql, Object... args);

    <T> T selectBean(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    <T> List<T> selectBeans(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    <T> PageResult<T> selectPage(@Language("SQL") final String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args);

    Map<String, Object> selectMap(@Language("SQL") final String sql, Object... args);

    List<Map<String, Object>> selectList(@Language("SQL") final String sql, Object... args);

    PageResult<Map<String, Object>> selectPage(@Language("SQL") final String sql, long pageNum, long pageSize, Object... args);
}