package io.github.shmilyjxs.utils;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.LinkedCaseInsensitiveMap;

import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BeanDaoContext extends SqlDaoContext {

    private static final Map<String, Map<String, String>> TABLE_CACHE = new HashMap<>();

    private static <T> String getTableName(T obj) {
        return Optional.of(obj).map(T::getClass).map(BeanDaoContext::getTableName).get();
    }

    private static <T> String getTableName(Class<T> clazz) {
        return Optional.of(clazz).map(e -> AnnotationUtils.findAnnotation(e, Table.class)).map(Table::name).filter(StringUtils::isNotBlank).get();
    }

    private static <T> Field pkFiled(T obj) {
        return Optional.of(obj).map(T::getClass).map(BeanDaoContext::pkFiled).get();
    }

    private static <T> Field pkFiled(Class<T> clazz) {
        return Optional.of(clazz).flatMap(e -> Arrays.stream(e.getDeclaredFields()).filter(i -> Objects.nonNull(AnnotationUtils.findAnnotation(i, Id.class))).findAny()).get();
    }

    private Map<String, String> convert(String tableName) {
        Map<String, String> result = TABLE_CACHE.get(tableName);
        if (Objects.isNull(result)) {
            synchronized (this) {
                result = TABLE_CACHE.get(tableName);
                if (Objects.isNull(result)) {
                    Map<String, String> instance = new LinkedCaseInsensitiveMap<>();
                    String sql = getDBEnum().getDialect().columnSql(tableName);
                    List<String> columnList = selectList(sql).stream().map(e -> (String) e.get("COLUMN_NAME")).collect(Collectors.toList());
                    columnList.forEach(e -> instance.put(e, e));
                    columnList.forEach(e -> {
                        String newKey = BeanUtil.dbToJava(e);
                        instance.putIfAbsent(newKey, e);
                    });
                    TABLE_CACHE.put(tableName, instance);
                    return instance;
                }
            }
        }
        return result;
    }

    @Override
    public <T> int insert(T obj, boolean skipBlank) {
        int result = 0;
        String tableName = getTableName(obj);
        Map<String, Object> beanMap = BeanUtil.beanToSortedMap(obj);
        if (skipBlank) {
            beanMap = BeanUtil.skipBlank(beanMap);
        }
        Map<String, String> convertMap = convert(tableName);
        List<String> columnList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        beanMap.forEach((key, val) -> Optional.ofNullable(convertMap.get(key)).ifPresent(e -> {
            columnList.add(e);
            valueList.add(val);
        }));
        if (columnList.size() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("INSERT INTO ");
            stringBuilder.append(tableName);
            stringBuilder.append(columnList.stream().collect(Collectors.joining(" , ", " ( ", " ) ")));
            stringBuilder.append("VALUES");
            stringBuilder.append(columnList.stream().map(e -> "?").collect(Collectors.joining(" , ", " ( ", " ) ")));
            result = nativeInsert(stringBuilder.toString(), valueList.toArray());
        }
        return result;
    }

    @Override
    public <T> int update(T obj, boolean skipBlank) {
        int result = 0;
        String tableName = getTableName(obj);
        Field pkFiled = pkFiled(obj);
        Map<String, Object> beanMap = BeanUtil.beanToSortedMap(obj);
        if (skipBlank) {
            beanMap = BeanUtil.skipBlank(beanMap);
        }
        Object pkValue = Objects.requireNonNull(beanMap.remove(pkFiled.getName()));
        Map<String, String> convertMap = convert(tableName);
        String pkName = Objects.requireNonNull(convertMap.get(pkFiled.getName()));
        List<String> columnList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        beanMap.forEach((key, val) -> Optional.ofNullable(convertMap.get(key)).ifPresent(e -> {
            columnList.add(e);
            valueList.add(val);
        }));
        if (columnList.size() > 0) {
            valueList.add(pkValue);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("UPDATE ");
            stringBuilder.append(tableName);
            stringBuilder.append(" SET ");
            stringBuilder.append(columnList.stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" , ")));
            stringBuilder.append(" WHERE ");
            stringBuilder.append(pkName.concat(" = ?"));
            result = nativeUpdate(stringBuilder.toString(), valueList.toArray());
        }
        return result;
    }

    @Override
    public <T> int delete(T obj) {
        try {
            String tableName = getTableName(obj);
            Field pkFiled = pkFiled(obj);
            pkFiled.setAccessible(true);
            Object pkValue = pkFiled.get(obj);
            String pkName = Objects.requireNonNull(convert(tableName).get(pkFiled.getName()));
            return delete(tableName, pkName, pkValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T, ID> int delete(ID pkValue, Class<T> mappedClass) {
        String tableName = getTableName(mappedClass);
        Field pkFiled = pkFiled(mappedClass);
        String pkName = Objects.requireNonNull(convert(tableName).get(pkFiled.getName()));
        return delete(tableName, pkName, pkValue);
    }

    @Override
    public <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass) {
        String tableName = getTableName(mappedClass);
        Field pkFiled = pkFiled(mappedClass);
        String pkName = Objects.requireNonNull(convert(tableName).get(pkFiled.getName()));
        return batchDelete(tableName, pkName, pkValues);
    }

    @Override
    public <T, ID> T getBean(ID pkValue, Class<T> mappedClass) {
        String tableName = getTableName(mappedClass);
        Field pkFiled = pkFiled(mappedClass);
        String pkName = Objects.requireNonNull(convert(tableName).get(pkFiled.getName()));
        return getBean(tableName, pkName, pkValue, mappedClass);
    }

    @Override
    public <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass) {
        String tableName = getTableName(mappedClass);
        Field pkFiled = pkFiled(mappedClass);
        String pkName = Objects.requireNonNull(convert(tableName).get(pkFiled.getName()));
        return getBeans(tableName, pkName, pkValues, mappedClass);
    }
}