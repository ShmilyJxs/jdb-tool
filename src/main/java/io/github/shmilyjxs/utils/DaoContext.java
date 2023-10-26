package io.github.shmilyjxs.utils;

import io.github.shmilyjxs.dialects.DBType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.intellij.lang.annotations.Language;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface DaoContext {

    DataSource getDataSource();

    JdbcTemplate getJdbcTemplate();

    NamedParameterJdbcTemplate getNamedJdbcTemplate();

    DBType getDBType();

    <T> Triple<String, Pair<Field, String>, Map<String, String>> getTableInfo(T obj);

    <T> Triple<String, Pair<Field, String>, Map<String, String>> getTableInfo(Class<T> clazz);

    Object idGenerator();

    <T> T scalar(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    <T> List<T> scalarList(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    long count(@Language("SQL") final String sql, Object... args);

    boolean exists(@Language("SQL") final String sql, Object... args);

    int nativeUpdate(@Language("SQL") final String sql, Object... args);

    <C> int delete(String tableName, String columnName, C columnValue);

    int delete(String tableName, Map<String, Object> columnMap);

    <C> int batchDelete(String tableName, String columnName, Collection<C> columnValues);

    <T> T selectBean(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    <T, C> T getBean(String tableName, String columnName, C columnValue, Class<T> mappedClass);

    <T> T getBean(String tableName, Map<String, Object> columnMap, Class<T> mappedClass);

    <T> List<T> selectBeans(@Language("SQL") final String sql, Class<T> mappedClass, Object... args);

    <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass, String... orderBy);

    <T> List<T> getBeans(String tableName, Map<String, Object> columnMap, Class<T> mappedClass, String... orderBy);

    <T> Map<String, Object> selectPage(@Language("SQL") final String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args);

    <T> Map<String, Object> selectPage(String tableName, Map<String, Object> columnMap, long pageNum, long pageSize, Class<T> mappedClass, String... orderBy);

    Map<String, Object> selectMap(@Language("SQL") final String sql, Object... args);

    <C> Map<String, Object> getMap(String tableName, String columnName, C columnValue);

    Map<String, Object> getMap(String tableName, Map<String, Object> columnMap);

    List<Map<String, Object>> selectList(@Language("SQL") final String sql, Object... args);

    <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues, String... orderBy);

    List<Map<String, Object>> getList(String tableName, Map<String, Object> columnMap, String... orderBy);

    Map<String, Object> selectPage(@Language("SQL") final String sql, long pageNum, long pageSize, Object... args);

    Map<String, Object> selectPage(String tableName, Map<String, Object> columnMap, long pageNum, long pageSize, String... orderBy);

    <T> T insert(T obj, boolean skipBlank);

    <T> int update(T obj, boolean skipBlank);

    <T> int deleteById(T obj);

    <T, ID> int delete(ID idValue, Class<T> mappedClass);

    <T, ID> int batchDelete(Collection<ID> idValues, Class<T> mappedClass);

    <T, ID> T getBean(ID idValue, Class<T> mappedClass);

    <T, ID> List<T> getBeans(Collection<ID> idValues, Class<T> mappedClass, String... orderBy);

    <T> int delete(T example);

    <T> T getBean(T example);

    <T> List<T> getBeans(T example, String... orderBy);

    <T> Map<String, Object> getPage(T example, long pageNum, long pageSize, String... orderBy);
}