package io.github.shmilyjxs.utils;

import com.google.common.collect.Lists;
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

    private static <C> String conditionSql(Map<String, Object> paramMap, String columnName, Collection<C> columnValues) {
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
        if (Objects.nonNull(columnValues) && columnValues.size() > 0) {
            Map<String, Object> paramMap = new HashMap<>();
            String sql = "DELETE FROM " + tableName + " WHERE " + conditionSql(paramMap, columnName, columnValues);
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
    public <T> List<T> getBeans(String tableName, Class<T> mappedClass) {
        String sql = "SELECT * FROM " + tableName;
        return selectBeans(sql, mappedClass);
    }

    @Override
    public <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass) {
        List<T> result = new ArrayList<>();
        if (Objects.nonNull(columnValues) && columnValues.size() > 0) {
            Map<String, Object> paramMap = new HashMap<>();
            String sql = "SELECT * FROM " + tableName + " WHERE " + conditionSql(paramMap, columnName, columnValues);
            logger.info(sql);
            result = getNamedJdbcTemplate().query(sql, paramMap, new BeanPropertyRowMapper<>(mappedClass));
        }
        return result;
    }

    @Override
    public <T> Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args) {
        pageNum = Math.max(pageNum, 1L);
        pageSize = Math.max(pageSize, 0L);

        Map<String, Object> result = new HashMap<>();
        List<T> records = new ArrayList<>();
        long total = count(sql, args);
        if (total > 0L) {
            if (pageSize > 0L) {
                long pages = total % pageSize == 0L ? total / pageSize : total / pageSize + 1L;
                result.put("pages", pages);
                if (pageNum <= pages) {
                    String pageSql = getDBEnum().getDialect().pageSql(sql, pageNum, pageSize);
                    records = selectBeans(pageSql, mappedClass, args);
                }
            }
        }
        long startRow = (pageNum - 1L) * pageSize + 1L;
        long endRow = Math.min(pageNum * pageSize, total);

        result.put("pageNum", pageNum);
        result.put("pageSize", pageSize);
        result.put("startRow", startRow);
        result.put("endRow", endRow);
        result.put("total", total);
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
    public List<Map<String, Object>> getList(String tableName) {
        String sql = "SELECT * FROM " + tableName;
        return selectList(sql);
    }

    @Override
    public <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (Objects.nonNull(columnValues) && columnValues.size() > 0) {
            Map<String, Object> paramMap = new HashMap<>();
            String sql = "SELECT * FROM " + tableName + " WHERE " + conditionSql(paramMap, columnName, columnValues);
            logger.info(sql);
            result = getNamedJdbcTemplate().queryForList(sql, paramMap);
        }
        return result;
    }

    @Override
    public Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Object... args) {
        pageNum = Math.max(pageNum, 1L);
        pageSize = Math.max(pageSize, 0L);

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> records = new ArrayList<>();
        long total = count(sql, args);
        if (total > 0L) {
            if (pageSize > 0L) {
                long pages = total % pageSize == 0L ? total / pageSize : total / pageSize + 1L;
                result.put("pages", pages);
                if (pageNum <= pages) {
                    String pageSql = getDBEnum().getDialect().pageSql(sql, pageNum, pageSize);
                    records = selectList(pageSql, args);
                }
            }
        }
        long startRow = (pageNum - 1L) * pageSize + 1L;
        long endRow = Math.min(pageNum * pageSize, total);

        result.put("pageNum", pageNum);
        result.put("pageSize", pageSize);
        result.put("startRow", startRow);
        result.put("endRow", endRow);
        result.put("total", total);
        result.put("records", records);
        return result;
    }
}