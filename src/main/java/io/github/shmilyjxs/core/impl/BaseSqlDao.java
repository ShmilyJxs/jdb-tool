package io.github.shmilyjxs.core.impl;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseSqlDao extends BaseNativeDao {

    private static final Logger logger = LoggerFactory.getLogger(BaseSqlDao.class);

    private static final String SELECT_PREFIX = "SELECT * FROM ";

    private static final String DELETE_PREFIX = "DELETE FROM ";

    private static final int SAFE_SIZE = 1000;

    private static Map.Entry<String, Object[]> buildSql(String prefix, String tableName, Map<String, ?> columnMap, String... orderBy) {
        Collection<?> valueList = Collections.emptyList();
        StringBuilder stringBuilder = new StringBuilder(prefix);
        stringBuilder.append(tableName);
        if (ObjectUtils.isNotEmpty(columnMap)) {
            stringBuilder.append(" WHERE ");
            stringBuilder.append(columnMap.keySet().stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" AND ")));
            valueList = columnMap.values();
        }
        Optional.ofNullable(orderBy)
                .filter(e -> e.length > 0)
                .map(e -> Arrays.stream(e).filter(StringUtils::isNotBlank).collect(Collectors.toList()))
                .filter(e -> e.size() > 0)
                .ifPresent(e -> {
                    stringBuilder.append(" ORDER BY ");
                    stringBuilder.append(String.join(" , ", e));
                });
        return new AbstractMap.SimpleImmutableEntry<>(stringBuilder.toString(), valueList.toArray());
    }

    private static <C> Map.Entry<String, Map<String, ?>> buildSql(String prefix, String tableName, String columnName, Collection<C> columnValues, String... orderBy) {
        StringBuilder stringBuilder = new StringBuilder(prefix);
        stringBuilder.append(tableName);
        stringBuilder.append(" WHERE ");
        Map<String, Object> paramMap = new HashMap<>();
        List<C> values = columnValues.stream().distinct().collect(Collectors.toList());
        if (values.size() > SAFE_SIZE) {
            List<List<C>> partitionList = Lists.partition(new ArrayList<>(values), SAFE_SIZE);
            String str = IntStream.range(0, partitionList.size()).mapToObj(index -> {
                String key = columnName + (index + 1);
                paramMap.put(key, partitionList.get(index));
                return new StringBuilder(columnName).append(" IN (:").append(key).append(")").toString();
            }).collect(Collectors.joining(" OR ", "( ", " )"));
            stringBuilder.append(str);
        } else if (values.size() > 1) {
            paramMap.put(columnName, values);
            stringBuilder.append(columnName).append(" IN (:").append(columnName).append(")");
        } else {
            paramMap.put(columnName, values.iterator().next());
            stringBuilder.append(columnName).append(" = :").append(columnName);
        }
        Optional.ofNullable(orderBy)
                .filter(e -> e.length > 0)
                .map(e -> Arrays.stream(e).filter(StringUtils::isNotBlank).collect(Collectors.toList()))
                .filter(e -> e.size() > 0)
                .ifPresent(e -> {
                    stringBuilder.append(" ORDER BY ");
                    stringBuilder.append(String.join(" , ", e));
                });
        return new AbstractMap.SimpleImmutableEntry<>(stringBuilder.toString(), paramMap);
    }

    @Override
    public int insert(String tableName, Map<String, ?> columnMap) {
        if (ObjectUtils.isNotEmpty(columnMap)) {
            StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");
            stringBuilder.append(tableName);
            stringBuilder.append(columnMap.keySet().stream().collect(Collectors.joining(" , ", " ( ", " ) ")));
            stringBuilder.append("VALUES");
            stringBuilder.append(columnMap.keySet().stream().map(e -> "?").collect(Collectors.joining(" , ", " ( ", " ) ")));
            return nativeUpdate(stringBuilder.toString(), columnMap.values().toArray());
        }
        return 0;
    }

    @Override
    public int update(String tableName, Map<String, ?> columnMap, String columnName) {
        return update(tableName, columnMap, Collections.singleton(columnName));
    }

    @Override
    public int update(String tableName, Map<String, ?> columnMap, Collection<String> columns) {
        if (ObjectUtils.isNotEmpty(columnMap)) {
            if (ObjectUtils.isNotEmpty(columns)) {
                Map<String, Object> map = new LinkedCaseInsensitiveMap<>(columnMap.size());
                map.putAll(columnMap);
                Map<String, ?> whereMap = columns.stream().map(String::toLowerCase).distinct().collect(Collectors.toMap(e -> e, map::remove));
                return update(tableName, map, whereMap);
            }
        }
        return 0;
    }

    @Override
    public int update(String tableName, Map<String, ?> columnMap, Map<String, ?> whereMap) {
        if (ObjectUtils.isNotEmpty(columnMap)) {
            if (ObjectUtils.isNotEmpty(whereMap)) {
                Map<String, Object> map = new LinkedCaseInsensitiveMap<>(whereMap.size());
                map.putAll(whereMap);
                Set<String> sets = columnMap.keySet();
                Set<String> keys = sets.stream()
                        .filter(map::containsKey)
                        .filter(e -> Objects.equals(columnMap.get(e), map.get(e)))
                        .collect(Collectors.toSet());
                Map<String, Object> setMap = new LinkedHashMap<>();
                sets.stream().filter(e -> keys.stream().noneMatch(key -> Objects.equals(key, e))).forEach(e -> setMap.put(e, columnMap.get(e)));
                if (ObjectUtils.isNotEmpty(setMap)) {
                    StringBuilder stringBuilder = new StringBuilder("UPDATE ");
                    stringBuilder.append(tableName);
                    stringBuilder.append(" SET ");
                    stringBuilder.append(setMap.keySet().stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" , ")));
                    stringBuilder.append(" WHERE ");
                    stringBuilder.append(map.keySet().stream().map(e -> e.concat(" = ?")).collect(Collectors.joining(" AND ")));
                    return nativeUpdate(stringBuilder.toString(), ArrayUtils.addAll(setMap.values().toArray(), map.values().toArray()));
                }
            }
        }
        return 0;
    }

    @Override
    public <C> int delete(String tableName, String columnName, C columnValue) {
        return delete(tableName, Collections.singletonMap(columnName, columnValue));
    }

    @Override
    public int delete(String tableName, Map<String, ?> columnMap) {
        if (ObjectUtils.isNotEmpty(columnMap)) {
            Map.Entry<String, Object[]> entry = buildSql(DELETE_PREFIX, tableName, columnMap);
            return nativeUpdate(entry.getKey(), entry.getValue());
        }
        return 0;
    }

    @Override
    public <C> int batchDelete(String tableName, String columnName, Collection<C> columnValues) {
        if (ObjectUtils.isNotEmpty(columnValues)) {
            Map.Entry<String, Map<String, ?>> entry = buildSql(DELETE_PREFIX, tableName, columnName, columnValues);
            String sql = entry.getKey();
            logger.info(sql);
            return getNamedJdbcTemplate().update(sql, entry.getValue());
        }
        return 0;
    }

    @Override
    public <T, C> T getBean(String tableName, String columnName, C columnValue, Class<T> mappedClass) {
        return getBean(tableName, Collections.singletonMap(columnName, columnValue), mappedClass);
    }

    @Override
    public <T> T getBean(String tableName, Map<String, ?> columnMap, Class<T> mappedClass) {
        if (ObjectUtils.isNotEmpty(columnMap)) {
            Map.Entry<String, Object[]> entry = buildSql(SELECT_PREFIX, tableName, columnMap);
            return selectBean(entry.getKey(), mappedClass, entry.getValue());
        }
        return null;
    }

    @Override
    public <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass, String... orderBy) {
        if (ObjectUtils.isNotEmpty(columnValues)) {
            Map.Entry<String, Map<String, ?>> entry = buildSql(SELECT_PREFIX, tableName, columnName, columnValues, orderBy);
            String sql = entry.getKey();
            logger.info(sql);
            return getNamedJdbcTemplate().query(sql, entry.getValue(), new BeanPropertyRowMapper<>(mappedClass));
        }
        return Collections.emptyList();
    }

    @Override
    public <T> List<T> getBeans(String tableName, Map<String, ?> columnMap, Class<T> mappedClass, String... orderBy) {
        Map.Entry<String, Object[]> entry = buildSql(SELECT_PREFIX, tableName, columnMap, orderBy);
        return selectBeans(entry.getKey(), mappedClass, entry.getValue());
    }

    @Override
    public <T> Map<String, Object> selectPage(String tableName, Map<String, ?> columnMap, long pageNum, long pageSize, Class<T> mappedClass, String... orderBy) {
        Map.Entry<String, Object[]> entry = buildSql(SELECT_PREFIX, tableName, columnMap, orderBy);
        return selectPage(entry.getKey(), pageNum, pageSize, mappedClass, entry.getValue());
    }

    @Override
    public <C> Map<String, Object> getMap(String tableName, String columnName, C columnValue) {
        return getMap(tableName, Collections.singletonMap(columnName, columnValue));
    }

    @Override
    public Map<String, Object> getMap(String tableName, Map<String, ?> columnMap) {
        if (ObjectUtils.isNotEmpty(columnMap)) {
            Map.Entry<String, Object[]> entry = buildSql(SELECT_PREFIX, tableName, columnMap);
            return selectMap(entry.getKey(), entry.getValue());
        }
        return null;
    }

    @Override
    public <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues, String... orderBy) {
        if (ObjectUtils.isNotEmpty(columnValues)) {
            Map.Entry<String, Map<String, ?>> entry = buildSql(SELECT_PREFIX, tableName, columnName, columnValues, orderBy);
            String sql = entry.getKey();
            logger.info(sql);
            return getNamedJdbcTemplate().queryForList(sql, entry.getValue());
        }
        return Collections.emptyList();
    }

    @Override
    public List<Map<String, Object>> getList(String tableName, Map<String, ?> columnMap, String... orderBy) {
        Map.Entry<String, Object[]> entry = buildSql(SELECT_PREFIX, tableName, columnMap, orderBy);
        return selectList(entry.getKey(), entry.getValue());
    }

    @Override
    public Map<String, Object> selectPage(String tableName, Map<String, ?> columnMap, long pageNum, long pageSize, String... orderBy) {
        Map.Entry<String, Object[]> entry = buildSql(SELECT_PREFIX, tableName, columnMap, orderBy);
        return selectPage(entry.getKey(), pageNum, pageSize, entry.getValue());
    }
}