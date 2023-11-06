package io.github.shmilyjxs.core;

import org.apache.commons.lang3.tuple.Triple;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IBeanDao {

    <T> Triple<String, Map.Entry<Field, String>, Map<String, String>> getTableInfo(T obj);

    <T> Triple<String, Map.Entry<Field, String>, Map<String, String>> getTableInfo(Class<T> clazz);

    <T> T insert(T obj, boolean skipBlank);

    <T> T updateById(T obj, boolean skipBlank);

    <T> T save(T obj, boolean skipBlank);

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