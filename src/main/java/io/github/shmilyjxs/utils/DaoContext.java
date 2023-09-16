package io.github.shmilyjxs.utils;

import io.github.shmilyjxs.dialects.DBEnum;
import org.intellij.lang.annotations.Language;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface DaoContext {

    DataSource getDataSource();

    JdbcTemplate getJdbcTemplate();

    NamedParameterJdbcTemplate getNamedJdbcTemplate();

    DBEnum getDBEnum();

    <T> T scalar(@Language("SQL") String sql, Class<T> mappedClass, Object... args);

    long count(@Language("SQL") String sql, Object... args);

    boolean exists(@Language("SQL") String sql, Object... args);

    int nativeInsert(@Language("SQL") String sql, Object... args);

    int nativeUpdate(@Language("SQL") String sql, Object... args);

    int nativeDelete(@Language("SQL") String sql, Object... args);

    <C> int delete(String tableName, String columnName, C columnValue);

    <C> int batchDelete(String tableName, String columnName, Collection<C> columnValues);

    <T> T selectBean(@Language("SQL") String sql, Class<T> mappedClass, Object... args);

    <T, C> T getBean(String tableName, String columnName, C columnValue, Class<T> mappedClass);

    <T> List<T> selectBeans(@Language("SQL") String sql, Class<T> mappedClass, Object... args);

    <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass);

    <T> Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args);

    Map<String, Object> selectMap(@Language("SQL") String sql, Object... args);

    <C> Map<String, Object> getMap(String tableName, String columnName, C columnValue);

    List<Map<String, Object>> selectList(@Language("SQL") String sql, Object... args);

    <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues);

    Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Object... args);

    <T> int insert(T obj, boolean skipBlank);

    <T> int update(T obj, boolean skipBlank);

    <T> int delete(T obj);

    <T, ID> int delete(ID pkValue, Class<T> mappedClass);

    <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass);

    <T, ID> T getBean(ID pkValue, Class<T> mappedClass);

    <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass);
}