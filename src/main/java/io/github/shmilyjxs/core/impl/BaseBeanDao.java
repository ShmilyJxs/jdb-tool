package io.github.shmilyjxs.core.impl;

import io.github.shmilyjxs.utils.BeanUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.LinkedCaseInsensitiveMap;
import org.springframework.util.ReflectionUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.*;

public abstract class BaseBeanDao extends BaseSqlDao {

    private static final Logger logger = LoggerFactory.getLogger(BaseBeanDao.class);

    private static final Map<Class<?>, Triple<String, Map.Entry<Field, String>, Map<String, String>>> CLASS_CACHE = new HashMap<>();

    private <T> Map<String, Object> buildMap(T obj, Map<String, String> convertMap) {
        return buildMap(obj, convertMap, true);
    }

    private <T> Map<String, Object> buildMap(T obj, Map<String, String> convertMap, boolean skipBlank) {
        Map<String, Object> beanMap = BeanUtil.beanToMap(obj);
        if (skipBlank) {
            beanMap = BeanUtil.skipBlank(beanMap);
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : convertMap.entrySet()) {
            if (beanMap.containsKey(entry.getKey())) {
                map.put(entry.getValue(), beanMap.get(entry.getKey()));
            }
        }
        return map;
    }

    @Override
    public <T> Triple<String, Map.Entry<Field, String>, Map<String, String>> getTableInfo(T obj) {
        return Optional.of(obj).map(T::getClass).map(this::getTableInfo).get();
    }

    @Override
    public <T> Triple<String, Map.Entry<Field, String>, Map<String, String>> getTableInfo(Class<T> clazz) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> triple = CLASS_CACHE.get(clazz);
        if (Objects.isNull(triple)) {
            synchronized (this) {
                triple = CLASS_CACHE.get(clazz);
                if (Objects.isNull(triple)) {
                    String tableName = BeanUtil.getTableName(clazz);
                    Field idFiled = BeanUtil.idFiled(clazz);

                    String sql = getDBType().getDialect().columnSql(tableName);
                    List<String> columnList = scalarList(sql, String.class);
                    Map<String, String> map = new LinkedCaseInsensitiveMap<>();
                    columnList.forEach(e -> map.put(e, e));
                    columnList.forEach(e -> map.putIfAbsent(JdbcUtils.convertUnderscoreNameToPropertyName(e), e));
                    columnList.forEach(e -> map.putIfAbsent(BeanUtil.dbToJava(e), e));

                    Map.Entry<Field, String> entry = new AbstractMap.SimpleImmutableEntry<>(idFiled, Objects.requireNonNull(map.get(idFiled.getName())));
                    Map<String, String> convertMap = new LinkedHashMap<>();
                    Arrays.stream(BeanUtils.getPropertyDescriptors(clazz)).map(PropertyDescriptor::getName).filter(map::containsKey).forEach(e -> convertMap.put(e, map.get(e)));
                    triple = Triple.of(tableName, entry, convertMap);
                    CLASS_CACHE.put(clazz, triple);
                }
            }
        }
        logger.info("class {} ===>>> table {}", clazz.getName(), triple.getLeft());
        logger.info("id {} ===>>> column {}", triple.getMiddle().getKey().getName(), triple.getMiddle().getValue());
        triple.getRight().forEach((key, val) -> logger.info("property {} ===>>> column {}", key, val));
        return triple;
    }

    @Override
    public <T> T insert(T obj, boolean skipBlank) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Field idField = tableInfo.getMiddle().getKey();
        ReflectionUtils.makeAccessible(idField);
        Object idValue = ReflectionUtils.getField(idField, obj);
        if (ObjectUtils.isEmpty(idValue)) {
            ReflectionUtils.setField(idField, obj, idGenerator());
        }
        insert(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), skipBlank));
        return obj;
    }

    @Override
    public <T> T updateById(T obj, boolean skipBlank) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        update(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), skipBlank), tableInfo.getMiddle().getValue());
        return obj;
    }

    @Override
    public <T> T insertOrUpdate(T obj, boolean skipBlank) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Field idField = tableInfo.getMiddle().getKey();
        ReflectionUtils.makeAccessible(idField);
        Object idValue = ReflectionUtils.getField(idField, obj);
        if (ObjectUtils.isEmpty(idValue)) {
            ReflectionUtils.setField(idField, obj, idGenerator());
            insert(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), skipBlank));
        } else {
            Map<String, Object> map = getMap(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue);
            if (Objects.isNull(map)) {
                insert(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), skipBlank));
            } else {
                update(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), skipBlank), tableInfo.getMiddle().getValue());
            }
        }
        return obj;
    }

    @Override
    public <T> int deleteById(T obj) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Field idFiled = tableInfo.getMiddle().getKey();
        ReflectionUtils.makeAccessible(idFiled);
        Object idValue = ReflectionUtils.getField(idFiled, obj);
        return delete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue);
    }

    @Override
    public <T, ID> int delete(ID idValue, Class<T> mappedClass) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return delete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue);
    }

    @Override
    public <T, ID> int batchDelete(Collection<ID> idValues, Class<T> mappedClass) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return batchDelete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValues);
    }

    @Override
    public <T, ID> T getBean(ID idValue, Class<T> mappedClass) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return getBean(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue, mappedClass);
    }

    @Override
    public <T, ID> List<T> getBeans(Collection<ID> idValues, Class<T> mappedClass, String... orderBy) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return getBeans(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValues, mappedClass, orderBy);
    }

    @Override
    public <T> int delete(T example) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return delete(tableInfo.getLeft(), buildMap(example, tableInfo.getRight()));
    }

    @Override
    public <T> T getBean(T example) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return getBean(tableInfo.getLeft(), buildMap(example, tableInfo.getRight()), (Class<T>) example.getClass());
    }

    @Override
    public <T> List<T> getBeans(T example, String... orderBy) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return getBeans(tableInfo.getLeft(), buildMap(example, tableInfo.getRight()), (Class<T>) example.getClass(), orderBy);
    }

    @Override
    public <T> Map<String, Object> getPage(T example, long pageNum, long pageSize, String... orderBy) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return selectPage(tableInfo.getLeft(), buildMap(example, tableInfo.getRight()), pageNum, pageSize, example.getClass(), orderBy);
    }
}