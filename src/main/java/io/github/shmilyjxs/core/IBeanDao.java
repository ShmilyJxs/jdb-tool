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

    <T> T insert(T obj, boolean skipBlank);

    void batchInsert(Collection<?> objs);

    <T> T updateById(T obj, boolean skipBlank);

    void batchUpdate(Collection<?> objs);

    <T> T insertOrUpdate(T obj, boolean skipBlank);

    void batchInsertOrUpdate(Collection<?> objs);

    <T> int deleteById(T obj);

    void batchDelete(Collection<?> objs);

    <T, ID> int delete(ID idValue, Class<T> mappedClass);

    <T, ID> int batchDelete(Collection<ID> idValues, Class<T> mappedClass);

    <T, ID> T getBean(ID idValue, Class<T> mappedClass);

    <T, ID> List<T> getBeans(Collection<ID> idValues, Class<T> mappedClass, String... orderBy);

    <T> int delete(T example);

    <T> T getBean(T example);

    <T> List<T> getBeans(T example, String... orderBy);

    <T> PageResult<T> getPage(T example, long pageNum, long pageSize, String... orderBy);
}