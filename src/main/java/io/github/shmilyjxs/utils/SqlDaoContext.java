package io.github.shmilyjxs.utils;

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
    default int nativeInsert(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    default int nativeUpdate(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    default int nativeDelete(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    default <C> int delete(String tableName, String columnName, C columnValue) {
        String sql = "DELETE FROM " + tableName + " WHERE " + columnName + " = ?";
        return nativeDelete(sql, columnValue);
    }

    @Override
    default <C> int batchDelete(String tableName, String columnName, Collection<C> columnValues) {
        int result = 0;
        if (Objects.nonNull(columnValues) && columnValues.size() > 0) {
            String sql = "DELETE FROM " + tableName + " WHERE " + columnName + " IN (:columnValues)";
            logger.info(sql);
            result = getNamedJdbcTemplate().update(sql, Collections.singletonMap("columnValues", columnValues));
        }
        return result;
    }

    @Override
    default <T> T selectBean(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        List<T> result = selectBeans(sql, mappedClass, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    default <T, C> T getBean(String tableName, String columnName, C columnValue, Class<T> mappedClass) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " = ?";
        return selectBean(sql, mappedClass, columnValue);
    }

    @Override
    default <T> List<T> selectBeans(@Language("SQL") String sql, Class<T> mappedClass, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(mappedClass), args);
    }

    @Override
    default <T> List<T> getBeans(String tableName, Class<T> mappedClass) {
        String sql = "SELECT * FROM " + tableName;
        return selectBeans(sql, mappedClass);
    }

    @Override
    default <T, C> List<T> getBeans(String tableName, String columnName, Collection<C> columnValues, Class<T> mappedClass) {
        List<T> result = new ArrayList<>();
        if (Objects.nonNull(columnValues) && columnValues.size() > 0) {
            String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " IN (:columnValues)";
            logger.info(sql);
            result = getNamedJdbcTemplate().query(sql, Collections.singletonMap("columnValues", columnValues), new BeanPropertyRowMapper<>(mappedClass));
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
    default <C> Map<String, Object> getMap(String tableName, String columnName, C columnValue) {
        String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " = ?";
        return selectMap(sql, columnValue);
    }

    @Override
    default List<Map<String, Object>> selectList(@Language("SQL") String sql, Object... args) {
        logger.info(sql);
        return getJdbcTemplate().queryForList(sql, args);
    }

    @Override
    default List<Map<String, Object>> getList(String tableName) {
        String sql = "SELECT * FROM " + tableName;
        return selectList(sql);
    }

    @Override
    default <C> List<Map<String, Object>> getList(String tableName, String columnName, Collection<C> columnValues) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (Objects.nonNull(columnValues) && columnValues.size() > 0) {
            String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " IN (:columnValues)";
            logger.info(sql);
            result = getNamedJdbcTemplate().queryForList(sql, Collections.singletonMap("columnValues", columnValues));
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