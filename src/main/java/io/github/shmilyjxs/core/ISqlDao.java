package io.github.shmilyjxs.core;

import io.github.shmilyjxs.utils.PageResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ISqlDao {

    int insert(String tableName, Map<String, ?> columnMap);

    int update(String tableName, Map<String, ?> columnMap, String columnName);

    int update(String tableName, Map<String, ?> columnMap, Collection<String> columns);

    int update(String tableName, Map<String, ?> columnMap, Map<String, ?> whereMap);

    <C> int delete(String tableName, String columnName, C columnValue);

    int delete(String tableName, Map<String, ?> columnMap);

    <C> int batchDelete(String tableName, String columnName, Collection<C> columnValues);

    <T, C> T getBean(String tableName, String columnName, C columnValue, Class<T> mappedClass);

    <T> T getBean(String tableName, Map<String, ?> columnMap, Class<T> mappedClass);

    <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass, String... orderBy);

    <T> List<T> getBeans(String tableName, Map<String, ?> columnMap, Class<T> mappedClass, String... orderBy);

    <T> PageResult<T> selectPage(String tableName, Map<String, ?> columnMap, long pageNum, long pageSize, Class<T> mappedClass, String... orderBy);

    <T, C> List<T> downRecursiveSql(String tableName, String startColumn, C columnValue, String joinColumn, Class<T> mappedClass, String... orderBy);

    <T, C> List<T> upRecursiveSql(String tableName, String startColumn, C columnValue, String joinColumn, Class<T> mappedClass, String... orderBy);

    <C> Map<String, Object> getMap(String tableName, String columnName, C columnValue);

    Map<String, Object> getMap(String tableName, Map<String, ?> columnMap);

    <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues, String... orderBy);

    List<Map<String, Object>> getList(String tableName, Map<String, ?> columnMap, String... orderBy);

    PageResult<Map<String, Object>> selectPage(String tableName, Map<String, ?> columnMap, long pageNum, long pageSize, String... orderBy);

    <C> List<Map<String, Object>> downRecursiveSql(String tableName, String startColumn, C columnValue, String joinColumn, String... orderBy);

    <C> List<Map<String, Object>> upRecursiveSql(String tableName, String startColumn, C columnValue, String joinColumn, String... orderBy);
}