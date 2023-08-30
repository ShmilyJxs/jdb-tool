package com.github.utils;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.intellij.lang.annotations.Language;
import org.springframework.core.annotation.AnnotationUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BeanDaoContext implements SqlDaoContext {

    private static final boolean JAVA_TO_DB = false;

    private static <T> String getTableName(Class<T> clazz) {
        return Optional.ofNullable(AnnotationUtils.findAnnotation(clazz, Table.class)).map(Table::name).filter(StringUtils::isNotBlank).orElse(null);
    }

    private static <T> Field pkFiled(Class<T> clazz) {
        return Arrays.stream(clazz.getDeclaredFields()).filter(e -> Objects.nonNull(AnnotationUtils.findAnnotation(e, Id.class))).findAny().orElse(null);
    }

    private static <T> Pair<Field, String> pkFiledMap(Class<T> clazz, boolean javaToDb) {
        return Optional.ofNullable(pkFiled(clazz)).map(e -> {
            String pkColumn = e.getName();
            if (javaToDb) {
                pkColumn = BeanUtil.javaToDb(pkColumn);
            }
            return new Pair<>(e, pkColumn);
        }).orElse(null);
    }

    private static <T> List<Field> ignoreFiled(Class<T> clazz) {
        return Arrays.stream(clazz.getDeclaredFields()).filter(e -> Objects.nonNull(AnnotationUtils.findAnnotation(e, Transient.class))).collect(Collectors.toList());
    }

    private static Pair<List<String>, List<Object>> toPair(Map<String, Object> map) {
        List<String> columnList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        Optional.ofNullable(map).ifPresent(e -> e.forEach((k, v) -> {
            columnList.add(k);
            valueList.add(v);
        }));
        return new Pair<>(columnList, valueList);
    }

    @Override
    public <T> int insert(T obj) {
        return insert(obj, true, JAVA_TO_DB);
    }

    @Override
    public <T> int insert(T obj, boolean skipBlank, boolean javaToDb) {
        int result = 0;
        if (Objects.nonNull(obj)) {
            Class<T> clazz = (Class<T>) obj.getClass();
            String tableName = getTableName(clazz);
            if (Objects.nonNull(tableName)) {
                Map<String, Object> dbMap = new LinkedHashMap<>();
                Set<String> ignoreSet = ignoreFiled(clazz).stream().map(Field::getName).collect(Collectors.toSet());
                for (Map.Entry<String, Object> entry : BeanUtil.beanToSortedMap(obj).entrySet()) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if (!ignoreSet.contains(key)) {
                        dbMap.put(key, val);
                    }
                }
                if (dbMap.size() > 0) {
                    if (skipBlank) {
                        dbMap = BeanUtil.skipBlank(dbMap);
                    }
                    if (dbMap.size() > 0) {
                        if (javaToDb) {
                            dbMap = BeanUtil.javaToDb(dbMap);
                        }
                        Pair<List<String>, List<Object>> pair = toPair(dbMap);
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("INSERT INTO ");
                        stringBuilder.append(tableName);
                        stringBuilder.append(pair.getKey().stream().collect(Collectors.joining(" , ", " ( ", " ) ")));
                        stringBuilder.append("VALUES");
                        stringBuilder.append(pair.getKey().stream().map(e -> "?").collect(Collectors.joining(" , ", " ( ", " ) ")));
                        result = insert(stringBuilder.toString(), pair.getValue().toArray());
                    }
                }
            }
        }
        return result;
    }

    @Override
    public <T> int update(T obj) {
        return update(obj, true, JAVA_TO_DB);
    }

    @Override
    public <T> int update(T obj, boolean skipBlank, boolean javaToDb) {
        int result = 0;
        if (Objects.nonNull(obj)) {
            Class<T> clazz = (Class<T>) obj.getClass();
            String tableName = getTableName(clazz);
            if (Objects.nonNull(tableName)) {
                Pair<Field, String> pkFiledMap = pkFiledMap(clazz, javaToDb);
                if (Objects.nonNull(pkFiledMap)) {
                    Map<String, Object> beanMap = BeanUtil.beanToSortedMap(obj);
                    String pkProperty = pkFiledMap.getKey().getName();
                    if (beanMap.containsKey(pkProperty)) {
                        Pair<String, Object> pkPair = new Pair<>(pkFiledMap.getValue(), beanMap.remove(pkProperty));
                        Map<String, Object> dbMap = new LinkedHashMap<>();
                        Set<String> ignoreSet = ignoreFiled(clazz).stream().map(Field::getName).collect(Collectors.toSet());
                        for (Map.Entry<String, Object> entry : beanMap.entrySet()) {
                            String key = entry.getKey();
                            Object val = entry.getValue();
                            if (!ignoreSet.contains(key)) {
                                dbMap.put(key, val);
                            }
                        }
                        if (dbMap.size() > 0) {
                            if (skipBlank) {
                                dbMap = BeanUtil.skipBlank(dbMap);
                            }
                            if (dbMap.size() > 0) {
                                if (javaToDb) {
                                    dbMap = BeanUtil.javaToDb(dbMap);
                                }
                                Pair<List<String>, List<Object>> pair = toPair(dbMap);
                                pair.getValue().add(pkPair.getValue());
                                StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append("UPDATE ");
                                stringBuilder.append(tableName);
                                stringBuilder.append(" SET ");
                                stringBuilder.append(pair.getKey().stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" , ")));
                                stringBuilder.append(" WHERE ");
                                stringBuilder.append(pkPair.getKey().concat(" = ?"));
                                result = update(stringBuilder.toString(), pair.getValue().toArray());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public <T> int delete(T obj) {
        return delete(obj, JAVA_TO_DB);
    }

    @Override
    public <T> int delete(T obj, boolean javaToDb) {
        try {
            int result = 0;
            if (Objects.nonNull(obj)) {
                Class<T> clazz = (Class<T>) obj.getClass();
                String tableName = getTableName(clazz);
                if (Objects.nonNull(tableName)) {
                    Pair<Field, String> pkFiledMap = pkFiledMap(clazz, javaToDb);
                    if (Objects.nonNull(pkFiledMap)) {
                        Field field = pkFiledMap.getKey();
                        field.setAccessible(true);
                        result = delete(tableName, pkFiledMap.getValue(), field.get(obj));
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T, ID> int delete(ID pkValue, Class<T> mappedClass) {
        return delete(pkValue, mappedClass, JAVA_TO_DB);
    }

    @Override
    public <T, ID> int delete(ID pkValue, Class<T> mappedClass, boolean javaToDb) {
        int result = 0;
        String tableName = getTableName(mappedClass);
        if (Objects.nonNull(tableName)) {
            Pair<Field, String> pkFiledMap = pkFiledMap(mappedClass, javaToDb);
            if (Objects.nonNull(pkFiledMap)) {
                result = delete(tableName, pkFiledMap.getValue(), pkValue);
            }
        }
        return result;
    }

    @Override
    public <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass) {
        return batchDelete(pkValues, mappedClass, JAVA_TO_DB);
    }

    @Override
    public <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass, boolean javaToDb) {
        int result = 0;
        if (Objects.nonNull(pkValues) && pkValues.size() > 0) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                Pair<Field, String> pkFiledMap = pkFiledMap(mappedClass, javaToDb);
                if (Objects.nonNull(pkFiledMap)) {
                    result = batchDelete(tableName, pkFiledMap.getValue(), pkValues);
                }
            }
        }
        return result;
    }

    @Override
    public <T, ID> T getBean(ID pkValue, Class<T> mappedClass) {
        return getBean(pkValue, mappedClass, JAVA_TO_DB);
    }

    @Override
    public <T, ID> T getBean(ID pkValue, Class<T> mappedClass, boolean javaToDb) {
        String tableName = getTableName(mappedClass);
        if (Objects.nonNull(tableName)) {
            Pair<Field, String> pkFiledMap = pkFiledMap(mappedClass, javaToDb);
            if (Objects.nonNull(pkFiledMap)) {
                return getBean(tableName, pkFiledMap.getValue(), pkValue, mappedClass);
            }
        }
        return null;
    }

    @Override
    public <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass) {
        return getBeans(pkValues, mappedClass, JAVA_TO_DB);
    }

    @Override
    public <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass, boolean javaToDb) {
        List<T> result = new ArrayList<>();
        if (Objects.nonNull(pkValues) && pkValues.size() > 0) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                Pair<Field, String> pkFiledMap = pkFiledMap(mappedClass, javaToDb);
                if (Objects.nonNull(pkFiledMap)) {
                    result = getBeans(tableName, pkFiledMap.getValue(), pkValues, mappedClass);
                }
            }
        }
        return result;
    }

    @Override
    public Map<String, Object> selectMap(@Language("SQL") String sql, boolean dbToJava, Object... args) {
        Map<String, Object> result = selectMap(sql, args);
        if (dbToJava) {
            result = BeanUtil.dbToJava(result);
        }
        return result;
    }

    @Override
    public <ID> Map<String, Object> getMap(String tableName, String pkColumn, ID pkValue, boolean dbToJava) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + pkColumn + " = ?";
        return selectMap(sql, dbToJava, new Object[]{pkValue});
    }

    @Override
    public List<Map<String, Object>> selectList(@Language("SQL") String sql, boolean dbToJava, Object... args) {
        List<Map<String, Object>> result = selectList(sql, args);
        if (dbToJava) {
            result = result.stream().map(BeanUtil::dbToJava).collect(Collectors.toList());
        }
        return result;
    }

    @Override
    public <ID> List<Map<String, Object>> getList(String tableName, String pkColumn, Collection<ID> pkValues, boolean dbToJava) {
        List<Map<String, Object>> result = getList(tableName, pkColumn, pkValues);
        if (dbToJava) {
            result = result.stream().map(BeanUtil::dbToJava).collect(Collectors.toList());
        }
        return result;
    }

    @Override
    public Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, boolean dbToJava, Object... args) {
        Map<String, Object> result = selectPage(sql, pageNum, pageSize, args);
        if (dbToJava) {
            List<Map<String, Object>> records = ((List<Map<String, Object>>) result.get("records")).stream().map(BeanUtil::dbToJava).collect(Collectors.toList());
            result.put("records", records);
        }
        return result;
    }
}