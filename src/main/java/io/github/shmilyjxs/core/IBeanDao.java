package io.github.shmilyjxs.core;

import io.github.shmilyjxs.utils.PageResult;
import org.apache.commons.lang3.tuple.Triple;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IBeanDao {

    <T> Triple<String, Map.Entry<Field, String>, Map<String, String>> getTableInfo(T obj);

    <T> Triple<String, Map.Entry<Field, String>, Map<String, String>> getTableInfo(Class<T> clazz);

    <T> int insert(T obj);

    void batchInsert(Collection<?> objs);

    <T> int updateById(T obj, boolean skipBlank);

    void batchUpdate(Collection<?> objs, boolean skipBlank);

    <T> int insertOrUpdate(T obj);

    void batchInsertOrUpdate(Collection<?> objs);

    <T> int deleteById(T obj);

    void batchDelete(Collection<?> objs);

    <T, ID> int delete(ID idValue, Class<T> mappedClass);

    <T, ID> int batchDelete(Collection<ID> idValues, Class<T> mappedClass);

    <T, ID> T getBean(ID idValue, Class<T> mappedClass);

    <T, ID> List<T> getBeans(Collection<ID> idValues, Class<T> mappedClass, String... lastSql);

    <T> int delete(T example);

    <T> T getBean(T example);

    <T> List<T> getBeans(T example, String... lastSql);

    <T> PageResult<T> getPage(T example, long pageNum, long pageSize, String... lastSql);
}