package io.github.shmilyjxs.utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ObjectUtils;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class SqlDaoContext implements DaoContext {

    private static final Logger logger = LoggerFactory.getLogger(SqlDaoContext.class);

    private static final int SAFE_SIZE = 1000;

    private static <C> String inSql(Map<String, Object> paramMap, String columnName, Collection<C> columnValues) {
        String sql;
        if (columnValues.size() > SAFE_SIZE) {
            List<List<C>> partitionList = Lists.partition(new ArrayList<>(columnValues), SAFE_SIZE);
            sql = IntStream.range(0, partitionList.size()).mapToObj(index -> {
                String key = columnName + index;
                paramMap.put(key, partitionList.get(index));
                return columnName + " IN (:" + key + ")";
            }).collect(Collectors.joining(" OR ", "( ", " )"));
        } else {
            paramMap.put(columnName, columnValues);
            sql = columnName + " IN (:" + columnName + ")";
        }
        return sql;
    }

    @Override
    public <T> T scalar(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().queryForObject(sql, mappedClass, args);
    }

    @Override
    public long count(@Language("SQL") String sql, Object... args) {
        String countSql = "SELECT COUNT(*) FROM ( " + sql + " ) tmp";
        logger.info(countSql);
        return getJdbcTemplate().queryForObject(countSql, Long.class, args);
    }

    @Override
    public boolean exists(@Language("SQL") String sql, Object... args) {
        return count(sql, args) > 0L;
    }

    @Override
    public int nativeInsert(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    public int nativeUpdate(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    public int nativeDelete(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    public <C> int delete(String tableName, String columnName, C columnValue) {
        String sql = "DELETE FROM " + tableName + " WHERE " + columnName + " = ?";
        return nativeDelete(sql, columnValue);
    }

    @Override
    public <C> int batchDelete(String tableName, String columnName, Collection<C> columnValues) {
        int result = 0;
        if (ObjectUtils.isNotEmpty(columnValues)) {
            Map<String, Object> paramMap = new HashMap<>();
            String sql = "DELETE FROM " + tableName + " WHERE " + inSql(paramMap, columnName, columnValues);
            logger.info(sql);
            result = getNamedJdbcTemplate().update(sql, paramMap);
        }
        return result;
    }

    @Override
    public <T> T selectBean(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        List<T> result = selectBeans(sql, mappedClass, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    public <T, C> T getBean(String tableName, String columnName, C columnValue, Class<T> mappedClass) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " = ?";
        return selectBean(sql, mappedClass, columnValue);
    }

    @Override
    public <T> List<T> selectBeans(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(mappedClass), args);
    }

    @Override
    public <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass) {
        List<T> result = Collections.emptyList();
        if (ObjectUtils.isNotEmpty(columnValues)) {
            Map<String, Object> paramMap = new HashMap<>();
            String sql = "SELECT * FROM " + tableName + " WHERE " + inSql(paramMap, columnName, columnValues);
            logger.info(sql);
            result = getNamedJdbcTemplate().query(sql, paramMap, new BeanPropertyRowMapper<>(mappedClass));
        }
        return result;
    }

    @Override
    public <T> Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args) {
        pageNum = Math.max(pageNum, 1L);
        pageSize = Math.max(pageSize, 0L);

        long total = count(sql, args);
        long pages = 0L;
        List<T> records = Collections.emptyList();
        if (total > 0L) {
            if (pageSize > 0L) {
                pages = total % pageSize == 0L ? total / pageSize : total / pageSize + 1L;
                if (pageNum <= pages) {
                    String pageSql = getDBEnum().getDialect().pageSql(sql, (pageNum - 1) * pageSize, pageSize);
                    records = selectBeans(pageSql, mappedClass, args);
                }
            }
        }

        Map<String, Object> result = new HashMap<>(5);
        result.put("pageNum", pageNum);
        result.put("pageSize", pageSize);
        result.put("total", total);
        result.put("pages", pages);
        result.put("records", records);
        return result;
    }

    @Override
    public Map<String, Object> selectMap(@Language("SQL") String sql, Object... args) {
        List<Map<String, Object>> result = selectList(sql, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    public <C> Map<String, Object> getMap(String tableName, String columnName, C columnValue) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " = ?";
        return selectMap(sql, columnValue);
    }

    @Override
    public List<Map<String, Object>> selectList(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().queryForList(sql, args);
    }

    @Override
    public <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues) {
        List<Map<String, Object>> result = Collections.emptyList();
        if (ObjectUtils.isNotEmpty(columnValues)) {
            Map<String, Object> paramMap = new HashMap<>();
            String sql = "SELECT * FROM " + tableName + " WHERE " + inSql(paramMap, columnName, columnValues);
            logger.info(sql);
            result = getNamedJdbcTemplate().queryForList(sql, paramMap);
        }
        return result;
    }

    @Override
    public Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Object... args) {
        pageNum = Math.max(pageNum, 1L);
        pageSize = Math.max(pageSize, 0L);

        long total = count(sql, args);
        long pages = 0L;
        List<Map<String, Object>> records = Collections.emptyList();
        if (total > 0L) {
            if (pageSize > 0L) {
                pages = total % pageSize == 0L ? total / pageSize : total / pageSize + 1L;
                if (pageNum <= pages) {
                    String pageSql = getDBEnum().getDialect().pageSql(sql, (pageNum - 1) * pageSize, pageSize);
                    records = selectList(pageSql, args);
                }
            }
        }

        Map<String, Object> result = new HashMap<>(5);
        result.put("pageNum", pageNum);
        result.put("pageSize", pageSize);
        result.put("total", total);
        result.put("pages", pages);
        result.put("records", records);
        return result;
    }
}