package io.github.shmilyjxs.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.LinkedCaseInsensitiveMap;
import org.springframework.util.ReflectionUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BeanDaoContext extends SqlDaoContext {

    private static final Map<Class<?>, Triple<String, Pair<Field, String>, Map<String, String>>> CLASS_CACHE = new HashMap<>();

    private static <T> String getTableName(T obj) {
        return Optional.of(obj).map(T::getClass).map(BeanDaoContext::getTableName).get();
    }

    private static <T> String getTableName(Class<T> clazz) {
        return Optional.of(clazz).map(e -> AnnotationUtils.findAnnotation(e, Table.class)).map(Table::name).filter(StringUtils::isNotBlank).orElseThrow(NullPointerException::new);
    }

    private static <T> Field idFiled(T obj) {
        return Optional.of(obj).map(T::getClass).map(BeanDaoContext::idFiled).get();
    }

    private static <T> Field idFiled(Class<T> clazz) {
        return Optional.of(clazz).flatMap(e -> Arrays.stream(e.getDeclaredFields()).filter(i -> Objects.nonNull(AnnotationUtils.findAnnotation(i, Id.class))).findAny()).orElseThrow(NullPointerException::new);
    }

    @Override
    public <T> Triple<String, Pair<Field, String>, Map<String, String>> getTableInfo(T obj) {
        return Optional.of(obj).map(T::getClass).map(this::getTableInfo).get();
    }

    @Override
    public <T> Triple<String, Pair<Field, String>, Map<String, String>> getTableInfo(Class<T> clazz) {
        Triple<String, Pair<Field, String>, Map<String, String>> triple = CLASS_CACHE.get(clazz);
        if (Objects.isNull(triple)) {
            synchronized (this) {
                triple = CLASS_CACHE.get(clazz);
                if (Objects.isNull(triple)) {
                    String tableName = getTableName(clazz);
                    Field idFiled = idFiled(clazz);

                    String sql = getDBType().getDialect().columnSql(tableName);
                    List<String> columnList = scalarList(sql, String.class);
                    Map<String, String> map = new LinkedCaseInsensitiveMap<>();
                    columnList.forEach(e -> map.put(e, e));
                    columnList.forEach(e -> map.putIfAbsent(BeanUtil.dbToJava(e), e));

                    Map<String, String> tableMap = new LinkedHashMap<>();
                    Arrays.stream(clazz.getDeclaredFields()).map(Field::getName).filter(map::containsKey).forEach(e -> tableMap.put(e, map.get(e)));

                    triple = Triple.of(tableName, Pair.of(idFiled, Objects.requireNonNull(tableMap.get(idFiled.getName()))), tableMap);
                    CLASS_CACHE.put(clazz, triple);
                }
            }
        }
        return triple;
    }

    @Override
    public Object idGenerator() {
        return UUID.randomUUID().toString();
    }

    @Override
    public <T> T insert(T obj, boolean skipBlank) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Map<String, Object> beanMap = BeanUtil.beanToLinkedMap(obj);
        if (skipBlank) {
            beanMap = BeanUtil.skipBlank(beanMap);
        }
        Field idField = tableInfo.getMiddle().getKey();
        Object idValue = beanMap.get(idField.getName());
        if (Objects.isNull(idValue)) {
            idValue = idGenerator();
            beanMap.put(idField.getName(), idValue);
            ReflectionUtils.makeAccessible(idField);
            ReflectionUtils.setField(idField, obj, idValue);
        }
        List<String> columnList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        Map<String, String> convertMap = tableInfo.getRight();
        beanMap.forEach((key, val) -> Optional.ofNullable(convertMap.get(key)).ifPresent(e -> {
            columnList.add(e);
            valueList.add(val);
        }));
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT INTO ");
        stringBuilder.append(tableInfo.getLeft());
        stringBuilder.append(columnList.stream().collect(Collectors.joining(" , ", " ( ", " ) ")));
        stringBuilder.append("VALUES");
        stringBuilder.append(columnList.stream().map(e -> "?").collect(Collectors.joining(" , ", " ( ", " ) ")));
        nativeUpdate(stringBuilder.toString(), valueList.toArray());
        return obj;
    }

    @Override
    public <T> int update(T obj, boolean skipBlank) {
        int result = 0;
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Map<String, Object> beanMap = BeanUtil.beanToLinkedMap(obj);
        if (skipBlank) {
            beanMap = BeanUtil.skipBlank(beanMap);
        }
        Object idValue = Objects.requireNonNull(beanMap.remove(tableInfo.getMiddle().getKey().getName()));
        List<String> columnList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        Map<String, String> convertMap = tableInfo.getRight();
        beanMap.forEach((key, val) -> Optional.ofNullable(convertMap.get(key)).ifPresent(e -> {
            columnList.add(e);
            valueList.add(val);
        }));
        if (columnList.size() > 0) {
            valueList.add(idValue);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("UPDATE ");
            stringBuilder.append(tableInfo.getLeft());
            stringBuilder.append(" SET ");
            stringBuilder.append(columnList.stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" , ")));
            stringBuilder.append(" WHERE ");
            stringBuilder.append(tableInfo.getMiddle().getValue().concat(" = ?"));
            result = nativeUpdate(stringBuilder.toString(), valueList.toArray());
        }
        return result;
    }

    @Override
    public <T> int deleteById(T obj) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Field idFiled = tableInfo.getMiddle().getKey();
        ReflectionUtils.makeAccessible(idFiled);
        Object idValue = ReflectionUtils.getField(idFiled, obj);
        return delete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue);
    }

    @Override
    public <T, ID> int delete(ID idValue, Class<T> mappedClass) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return delete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue);
    }

    @Override
    public <T, ID> int batchDelete(Collection<ID> idValues, Class<T> mappedClass) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return batchDelete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValues);
    }

    @Override
    public <T, ID> T getBean(ID idValue, Class<T> mappedClass) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return getBean(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue, mappedClass);
    }

    @Override
    public <T, ID> List<T> getBeans(Collection<ID> idValues, Class<T> mappedClass, String... orderBy) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(mappedClass);
        return getBeans(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValues, mappedClass, orderBy);
    }

    @Override
    public <T> int delete(T example) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return delete(tableInfo.getLeft(), buildMap(example));
    }

    @Override
    public <T> T getBean(T example) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return getBean(tableInfo.getLeft(), buildMap(example), (Class<T>) example.getClass());
    }

    @Override
    public <T> List<T> getBeans(T example, String... orderBy) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return getBeans(tableInfo.getLeft(), buildMap(example), (Class<T>) example.getClass(), orderBy);
    }

    @Override
    public <T> Map<String, Object> getPage(T example, long pageNum, long pageSize, String... orderBy) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return selectPage(tableInfo.getLeft(), buildMap(example), pageNum, pageSize, example.getClass(), orderBy);
    }

    private <T> Map<String, Object> buildMap(T obj) {
        Triple<String, Pair<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Map<String, Object> beanMap = BeanUtil.beanToLinkedMap(obj);
        beanMap = BeanUtil.skipBlank(beanMap);
        Map<String, String> convertMap = tableInfo.getRight();
        return beanMap.entrySet().stream().filter(e -> convertMap.containsKey(e.getKey())).collect(Collectors.toMap(e -> convertMap.get(e.getKey()), Map.Entry::getValue));
    }
}