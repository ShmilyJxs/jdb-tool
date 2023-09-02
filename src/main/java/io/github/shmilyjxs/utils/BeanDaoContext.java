package io.github.shmilyjxs.utils;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.LinkedCaseInsensitiveMap;

import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public abstract class BeanDaoContext implements SqlDaoContext {

    private static final Map<String, Map<String, String>> TABLE_CACHE = new HashMap<>();

    private static <T> String getTableName(Class<T> clazz) {
        return Optional.ofNullable(AnnotationUtils.findAnnotation(clazz, Table.class)).map(Table::name).filter(StringUtils::isNotBlank).orElse(null);
    }

    private static <T> Field pkFiled(Class<T> clazz) {
        return Arrays.stream(clazz.getDeclaredFields()).filter(e -> Objects.nonNull(AnnotationUtils.findAnnotation(e, Id.class))).findAny().orElse(null);
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
        if (Objects.nonNull(obj)) {
            Class<T> clazz = (Class<T>) obj.getClass();
            String tableName = getTableName(clazz);
            if (Objects.nonNull(tableName)) {
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
            }
        }
        return result;
    }

    @Override
    public <T> int update(T obj, boolean skipBlank) {
        int result = 0;
        if (Objects.nonNull(obj)) {
            Class<T> clazz = (Class<T>) obj.getClass();
            String tableName = getTableName(clazz);
            if (Objects.nonNull(tableName)) {
                Field pkFiled = pkFiled(clazz);
                if (Objects.nonNull(pkFiled)) {
                    Map<String, Object> beanMap = BeanUtil.beanToSortedMap(obj);
                    if (skipBlank) {
                        beanMap = BeanUtil.skipBlank(beanMap);
                    }
                    Object pkValue = beanMap.remove(pkFiled.getName());
                    if (Objects.nonNull(pkValue)) {
                        Map<String, String> convertMap = convert(tableName);
                        Pair<String, Object> pkPair = Optional.ofNullable(convertMap.get(pkFiled.getName())).map(e -> new Pair<>(e, pkValue)).orElse(null);
                        if (Objects.nonNull(pkPair)) {
                            List<String> columnList = new ArrayList<>();
                            List<Object> valueList = new ArrayList<>();
                            beanMap.forEach((key, val) -> Optional.ofNullable(convertMap.get(key)).ifPresent(e -> {
                                columnList.add(e);
                                valueList.add(val);
                            }));
                            if (columnList.size() > 0) {
                                valueList.add(pkPair.getValue());
                                StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append("UPDATE ");
                                stringBuilder.append(tableName);
                                stringBuilder.append(" SET ");
                                stringBuilder.append(columnList.stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" , ")));
                                stringBuilder.append(" WHERE ");
                                stringBuilder.append(pkPair.getKey().concat(" = ?"));
                                result = nativeUpdate(stringBuilder.toString(), valueList.toArray());
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
        try {
            int result = 0;
            if (Objects.nonNull(obj)) {
                Class<T> clazz = (Class<T>) obj.getClass();
                String tableName = getTableName(clazz);
                if (Objects.nonNull(tableName)) {
                    Field pkFiled = pkFiled(clazz);
                    if (Objects.nonNull(pkFiled)) {
                        pkFiled.setAccessible(true);
                        Object pkValue = pkFiled.get(obj);
                        if (Objects.nonNull(pkValue)) {
                            String pkName = Optional.of(convert(tableName)).map(e -> e.get(pkFiled.getName())).orElse(null);
                            if (Objects.nonNull(pkName)) {
                                result = delete(tableName, pkName, pkValue);
                            }
                        }
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
        int result = 0;
        if (Objects.nonNull(pkValue) && Objects.nonNull(mappedClass)) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                Field pkFiled = pkFiled(mappedClass);
                if (Objects.nonNull(pkFiled)) {
                    String pkName = Optional.of(convert(tableName)).map(e -> e.get(pkFiled.getName())).orElse(null);
                    if (Objects.nonNull(pkName)) {
                        result = delete(tableName, pkName, pkValue);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public <T, ID> int batchDelete(Collection<ID> pkValues, Class<T> mappedClass) {
        int result = 0;
        if (Objects.nonNull(pkValues) && Objects.nonNull(mappedClass) && pkValues.size() > 0) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                Field pkFiled = pkFiled(mappedClass);
                if (Objects.nonNull(pkFiled)) {
                    String pkName = Optional.of(convert(tableName)).map(e -> e.get(pkFiled.getName())).orElse(null);
                    if (Objects.nonNull(pkName)) {
                        result = batchDelete(tableName, pkName, pkValues);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public <T, ID> T getBean(ID pkValue, Class<T> mappedClass) {
        if (Objects.nonNull(pkValue) && Objects.nonNull(mappedClass)) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                Field pkFiled = pkFiled(mappedClass);
                if (Objects.nonNull(pkFiled)) {
                    String pkName = Optional.of(convert(tableName)).map(e -> e.get(pkFiled.getName())).orElse(null);
                    if (Objects.nonNull(pkName)) {
                        return getBean(tableName, pkName, pkValue, mappedClass);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public <T> List<T> getBeans(Class<T> mappedClass) {
        List<T> result = new ArrayList<>();
        if (Objects.nonNull(mappedClass)) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                result = getBeans(tableName, mappedClass);
            }
        }
        return result;
    }

    @Override
    public <T, ID> List<T> getBeans(Collection<ID> pkValues, Class<T> mappedClass) {
        List<T> result = new ArrayList<>();
        if (Objects.nonNull(pkValues) && Objects.nonNull(mappedClass) && pkValues.size() > 0) {
            String tableName = getTableName(mappedClass);
            if (Objects.nonNull(tableName)) {
                Field pkFiled = pkFiled(mappedClass);
                if (Objects.nonNull(pkFiled)) {
                    String pkName = Optional.of(convert(tableName)).map(e -> e.get(pkFiled.getName())).orElse(null);
                    if (Objects.nonNull(pkName)) {
                        result = getBeans(tableName, pkName, pkValues, mappedClass);
                    }
                }
            }
        }
        return result;
    }
}