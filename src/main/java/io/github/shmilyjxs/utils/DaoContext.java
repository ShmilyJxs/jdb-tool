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

    long count(@Language("SQL") String sql, Object... args);

    boolean exists(@Language("SQL") String sql, Object... args);

    int nativeInsert(@Language("SQL") String sql, Object... args);

    int nativeUpdate(@Language("SQL") String sql, Object... args);

    int nativeDelete(@Language("SQL") String sql, Object... args);

    <ID> int delete(String tableName, String pkColumn, ID pkValue);

    <ID> int batchDelete(String tableName, String pkColumn, Collection<ID> pkValues);

    <T> T selectBean(@Language("SQL") String sql, Class<T> mappedClass, Object... args);

    <T, ID> T getBean(String tableName, String pkColumn, ID pkValue, Class<T> mappedClass);

    <T> List<T> selectBeans(@Language("SQL") String sql, Class<T> mappedClass, Object... args);

    <T, ID> List<T> getBeans(String tableName, String pkColumn, Collection<ID> pkValues, Class<T> mappedClass);

    <T> Map<String, Object> selectPage(@Language("SQL") String sql, Class<T> mappedClass, long pageNum, long pageSize, Object... args);

    Map<String, Object> selectMap(@Language("SQL") String sql, Object... args);

    <ID> Map<String, Object> getMap(String tableName, String pkColumn, ID pkValue);

    List<Map<String, Object>> selectList(@Language("SQL") String sql, Object... args);

    <ID> List<Map<String, Object>> getList(String tableName, String pkColumn, Collection<ID> pkValues);

    Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Object... args);

    <T> int insert(T obj);

    <T> int insert(T obj, boolean skipBlank, boolean javaToDb);

    <T> int update(T obj);

    <T> int update(T obj, boolean skipBlank, boolean javaToDb);

    <T> int delete(T obj);

    <T> int delete(T obj, boolean javaToDb);

    <T, ID> int delete(ID pkValue, Class<T> mappedClass);

    <T, ID> int delete(ID pkValue, Class<T> mappedClass, boolean javaToDb);

    <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass);

    <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass, boolean javaToDb);

    <T, ID> T getBean(ID pkValue, Class<T> mappedClass);

    <T, ID> T getBean(ID pkValue, Class<T> mappedClass, boolean javaToDb);

    <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass);

    <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass, boolean javaToDb);
}