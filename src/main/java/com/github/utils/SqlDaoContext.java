package com.github.utils;

import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.*;

public interface SqlDaoContext extends DaoContext {

    Logger logger = LoggerFactory.getLogger(SqlDaoContext.class);

    @Override
    default long count(@Language("SQL") String sql, Object... args) {
        String countSql = "SELECT COUNT(*) FROM ( " + sql + " ) tmp";
        logger.info(countSql);
        return getJdbcTemplate().queryForObject(countSql, Long.class, args);
    }

    @Override
    default boolean exists(@Language("SQL") String sql, Object... args) {
        return count(sql, args) > 0L;
    }

    @Override
    default int insert(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    default int update(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    default int delete(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    default <ID> int delete(String tableName, String pkColumn, ID pkValue) {
        String sql = "DELETE FROM " + tableName + " WHERE " + pkColumn + " = ?";
        return delete(sql, pkValue);
    }

    @Override
    default <ID> int batchDelete(String tableName, String pkColumn, Collection<ID> pkValues) {
        int result = 0;
        if (Objects.nonNull(pkValues) && pkValues.size() > 0) {
            String sql = "DELETE FROM " + tableName + " WHERE " + pkColumn + " IN (:pkValues)";
            logger.info(sql);
            result = getNamedJdbcTemplate().update(sql, Collections.singletonMap("pkValues", pkValues));
        }
        return result;
    }

    @Override
    default <T> T selectBean(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        List<T> result = selectBeans(sql, mappedClass, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    default <T, ID> T getBean(String tableName, String pkColumn, ID pkValue, Class<T> mappedClass) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + pkColumn + " = ?";
        return selectBean(sql, mappedClass, pkValue);
    }

    @Override
    default <T> List<T> selectBeans(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(mappedClass), args);
    }

    @Override
    default <T, ID> List<T> getBeans(String tableName, String pkColumn, Collection<ID> pkValues, Class<T> mappedClass) {
        List<T> result = new ArrayList<>();
        if (Objects.nonNull(pkValues) && pkValues.size() > 0) {
            String sql = "SELECT * FROM " + tableName + " WHERE " + pkColumn + " IN (:pkValues)";
            logger.info(sql);
            result = getNamedJdbcTemplate().query(sql, Collections.singletonMap("pkValues", pkValues), new BeanPropertyRowMapper<>(mappedClass));
        }
        return result;
    }

    @Override
    default <T> Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args) {
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
        long rowStart = (pageNum - 1L) * pageSize + 1L;
        long rowEnd = Math.min(pageNum * pageSize, total);

        result.put("pageNum", pageNum);
        result.put("pageSize", pageSize);
        result.put("rowStart", rowStart);
        result.put("rowEnd", rowEnd);
        result.put("total", total);
        result.put("records", records);
        return result;
    }

    @Override
    default Map<String, Object> selectMap(@Language("SQL") String sql, Object... args) {
        List<Map<String, Object>> result = selectList(sql, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    default <ID> Map<String, Object> getMap(String tableName, String pkColumn, ID pkValue) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + pkColumn + " = ?";
        return selectMap(sql, pkValue);
    }

    @Override
    default List<Map<String, Object>> selectList(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().queryForList(sql, args);
    }

    @Override
    default <ID> List<Map<String, Object>> getList(String tableName, String pkColumn, Collection<ID> pkValues) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (Objects.nonNull(pkValues) && pkValues.size() > 0) {
            String sql = "SELECT * FROM " + tableName + " WHERE " + pkColumn + " IN (:pkValues)";
            logger.info(sql);
            result = getNamedJdbcTemplate().queryForList(sql, Collections.singletonMap("pkValues", pkValues));
        }
        return result;
    }

    @Override
    default Map<String, Object> selectPage(@Language("SQL") String sql, long pageNum, long pageSize, Object... args) {
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
        long rowStart = (pageNum - 1L) * pageSize + 1L;
        long rowEnd = Math.min(pageNum * pageSize, total);

        result.put("pageNum", pageNum);
        result.put("pageSize", pageSize);
        result.put("rowStart", rowStart);
        result.put("rowEnd", rowEnd);
        result.put("total", total);
        result.put("records", records);
        return result;
    }
}