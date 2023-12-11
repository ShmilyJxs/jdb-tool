package io.github.shmilyjxs.core.impl;

import io.github.shmilyjxs.utils.BeanUtil;
import io.github.shmilyjxs.utils.PageResult;
import org.apache.commons.lang3.ArrayUtils;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class BaseBeanDao extends BaseSqlDao {

    private static final Logger logger = LoggerFactory.getLogger(BaseBeanDao.class);

    private static final Map<Class<?>, Triple<String, Map.Entry<Field, String>, Map<String, String>>> CLASS_CACHE = new ConcurrentHashMap<>();

    private static <T> Map<String, Object> buildMap(T obj, Map<String, String> convertMap) {
        return buildMap(obj, convertMap, true);
    }

    private static <T> Map<String, Object> buildMap(T obj, Map<String, String> convertMap, boolean skipBlank) {
        Map<String, Object> beanMap = BeanUtil.beanToMap(obj);
        if (skipBlank) {
            beanMap = BeanUtil.skipBlank(beanMap);
        }
        Map<String, Object> map = new LinkedCaseInsensitiveMap<>();
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
                    Arrays.stream(BeanUtils.getPropertyDescriptors(clazz))
                            .map(PropertyDescriptor::getName)
                            .filter(map::containsKey)
                            .map(e -> new AbstractMap.SimpleImmutableEntry<>(e, map.get(e)))
                            .sorted(Comparator.comparingInt(e -> columnList.indexOf(e.getValue())))
                            .forEach(e -> convertMap.put(e.getKey(), e.getValue()));
                    triple = Triple.of(tableName, entry, Collections.unmodifiableMap(convertMap));
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
    public <T> int insert(T obj) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Field idField = tableInfo.getMiddle().getKey();
        ReflectionUtils.makeAccessible(idField);
        Object idValue = ReflectionUtils.getField(idField, obj);
        if (ObjectUtils.isEmpty(idValue)) {
            ReflectionUtils.setField(idField, obj, idGenerator());
        }
        return insert(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), false));
    }

    @Override
    public void batchInsert(Collection<?> objs) {
        objs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(Object::getClass)).forEach((key, val) -> {
            Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(key);
            List<Map<String, Object>> maps = new ArrayList<>();
            Field idField = tableInfo.getMiddle().getKey();
            ReflectionUtils.makeAccessible(idField);
            val.forEach(obj -> {
                Object idValue = ReflectionUtils.getField(idField, obj);
                if (ObjectUtils.isEmpty(idValue)) {
                    ReflectionUtils.setField(idField, obj, idGenerator());
                }
                Map<String, Object> map = buildMap(obj, tableInfo.getRight(), false);
                maps.add(map);
            });
            batchInsert(tableInfo.getLeft(), tableInfo.getRight().values(), maps);
        });
    }

    @Override
    public <T> int updateById(T obj, boolean skipBlank) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        return update(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), skipBlank), tableInfo.getMiddle().getValue());
    }

    @Override
    public void batchUpdate(Collection<?> objs, boolean skipBlank) {
        objs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(Object::getClass)).forEach((key, val) -> {
            Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(key);
            String idColumn = tableInfo.getMiddle().getValue();
            StringBuilder stringBuilder = new StringBuilder("UPDATE ");
            stringBuilder.append(tableInfo.getLeft());
            stringBuilder.append(" SET ");
            stringBuilder.append(tableInfo.getRight().values().stream().filter(e -> ObjectUtils.notEqual(e, idColumn)).map(e -> e.concat(" = ?")).collect(Collectors.joining(" , ")));
            stringBuilder.append(" WHERE ");
            stringBuilder.append(idColumn.concat(" = ?"));
            String sql = stringBuilder.toString();
            List<Object[]> batchArgs = new ArrayList<>();
            List<Map<String, Object>> maps = val.stream().map(e -> buildMap(e, tableInfo.getRight(), skipBlank)).collect(Collectors.toList());
            if (skipBlank) {
                List<?> idList = maps.stream().map(e -> e.get(idColumn)).collect(Collectors.toList());
                Map<?, List<Map<String, Object>>> idGroup = getList(tableInfo.getLeft(), idColumn, idList).stream().collect(Collectors.groupingBy(e -> e.get(idColumn)));
                maps.forEach(map -> {
                    Object idValue = map.get(idColumn);
                    Map<String, Object> dbMap = idGroup.get(idValue).get(0);
                    dbMap.putAll(map);
                    Object[] objects = tableInfo.getRight().values().stream().filter(e -> ObjectUtils.notEqual(e, idColumn)).map(dbMap::get).toArray();
                    batchArgs.add(ArrayUtils.add(objects, idValue));
                });
            } else {
                maps.forEach(map -> {
                    Object idValue = map.get(idColumn);
                    Object[] objects = tableInfo.getRight().values().stream().filter(e -> ObjectUtils.notEqual(e, idColumn)).map(map::get).toArray();
                    batchArgs.add(ArrayUtils.add(objects, idValue));
                });
            }
            logger.info("sql = {}", sql);
            batchArgs.forEach(e -> logger.info("args = {}", Arrays.asList(e)));
            getJdbcTemplate().batchUpdate(sql, batchArgs);
        });
    }

    @Override
    public <T> int insertOrUpdate(T obj) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(obj);
        Field idField = tableInfo.getMiddle().getKey();
        ReflectionUtils.makeAccessible(idField);
        Object idValue = ReflectionUtils.getField(idField, obj);
        if (ObjectUtils.isEmpty(idValue)) {
            ReflectionUtils.setField(idField, obj, idGenerator());
            return insert(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), false));
        } else {
            Map<String, Object> map = getMap(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValue);
            if (Objects.isNull(map)) {
                return insert(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), false));
            } else {
                return update(tableInfo.getLeft(), buildMap(obj, tableInfo.getRight(), false), tableInfo.getMiddle().getValue());
            }
        }
    }

    @Override
    public void batchInsertOrUpdate(Collection<?> objs) {
        List<Object> insertList = new ArrayList<>();
        List<Object> updateList = new ArrayList<>();
        objs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(Object::getClass)).forEach((key, val) -> {
            Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(key);
            Field idField = tableInfo.getMiddle().getKey();
            ReflectionUtils.makeAccessible(idField);
            List<?> idList = val.stream().map(e -> ReflectionUtils.getField(idField, e)).filter(ObjectUtils::isNotEmpty).collect(Collectors.toList());
            Map<?, List<Map<String, Object>>> idGroup = getList(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idList).stream().collect(Collectors.groupingBy(e -> e.get(tableInfo.getMiddle().getValue())));
            val.forEach(obj -> {
                Object idValue = ReflectionUtils.getField(idField, obj);
                if (ObjectUtils.isEmpty(idValue)) {
                    insertList.add(obj);
                } else if (idGroup.containsKey(idValue)) {
                    updateList.add(obj);
                } else {
                    insertList.add(obj);
                }
            });
        });
        batchInsert(insertList);
        batchUpdate(updateList, false);
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
    public void batchDelete(Collection<?> objs) {
        objs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(Object::getClass)).forEach((key, val) -> {
            Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(key);
            Field idFiled = tableInfo.getMiddle().getKey();
            ReflectionUtils.makeAccessible(idFiled);
            List<?> idValues = val.stream().map(e -> ReflectionUtils.getField(idFiled, e)).collect(Collectors.toList());
            batchDelete(tableInfo.getLeft(), tableInfo.getMiddle().getValue(), idValues);
        });
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
    public <T> PageResult<T> getPage(T example, long pageNum, long pageSize, String... orderBy) {
        Triple<String, Map.Entry<Field, String>, Map<String, String>> tableInfo = getTableInfo(example);
        return selectPage(tableInfo.getLeft(), buildMap(example, tableInfo.getRight()), pageNum, pageSize, (Class<T>) example.getClass(), orderBy);
    }
}