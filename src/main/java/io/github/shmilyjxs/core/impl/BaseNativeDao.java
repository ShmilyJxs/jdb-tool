package io.github.shmilyjxs.core.impl;

import io.github.shmilyjxs.core.IDaoContext;
import io.github.shmilyjxs.utils.PageResult;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import java.util.*;

public abstract class BaseNativeDao implements IDaoContext {

    private static final Logger logger = LoggerFactory.getLogger(BaseNativeDao.class);

    @Override
    public <T> T scalar(@Language("SQL") final String sql, Class<T> mappedClass, Object... args) {
        logger.info("sql = {}", sql);
        Optional.ofNullable(args).filter(e -> e.length > 0).map(Arrays::asList).ifPresent(e -> logger.info("args = {}", e));
        return getJdbcTemplate().queryForObject(sql, mappedClass, args);
    }

    @Override
    public <T> List<T> scalarList(@Language("SQL") final String sql, Class<T> mappedClass, Object... args) {
        logger.info("sql = {}", sql);
        Optional.ofNullable(args).filter(e -> e.length > 0).map(Arrays::asList).ifPresent(e -> logger.info("args = {}", e));
        return getJdbcTemplate().queryForList(sql, mappedClass, args);
    }

    @Override
    public long count(@Language("SQL") final String sql, Object... args) {
        return scalar("SELECT COUNT(*) FROM ( " + sql + " ) tmp", Long.class, args);
    }

    @Override
    public boolean exists(@Language("SQL") final String sql, Object... args) {
        return count(sql, args) > 0L;
    }

    @Override
    public int nativeUpdate(@Language("SQL") final String sql, Object... args) {
        logger.info("sql = {}", sql);
        Optional.ofNullable(args).filter(e -> e.length > 0).map(Arrays::asList).ifPresent(e -> logger.info("args = {}", e));
        return getJdbcTemplate().update(sql, args);
    }

    @Override
    public <T> T selectBean(@Language("SQL") final String sql, Class<T> mappedClass, Object... args) {
        List<T> result = selectBeans(sql, mappedClass, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    public <T> List<T> selectBeans(@Language("SQL") final String sql, Class<T> mappedClass, Object... args) {
        logger.info("sql = {}", sql);
        Optional.ofNullable(args).filter(e -> e.length > 0).map(Arrays::asList).ifPresent(e -> logger.info("args = {}", e));
        return getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(mappedClass), args);
    }

    @Override
    public <T> PageResult<T> selectPage(@Language("SQL") final String sql, long pageNum, long pageSize, Class<T> mappedClass, Object... args) {
        long total = count(sql, args);
        long pages = 0L;
        List<T> records = Collections.emptyList();
        if (total > 0L) {
            if (pageSize > 0L) {
                pages = (total / pageSize) + (total % pageSize == 0L ? 0L : 1L);
                if (pageNum > 0L && pageNum <= pages) {
                    String pageSql = getDBType().getDialect().pageSql(sql, (pageNum - 1L) * pageSize, pageSize);
                    records = selectBeans(pageSql, mappedClass, args);
                }
            }
        }
        return PageResult.of(pageNum, pageSize, total, pages, records);
    }

    @Override
    public Map<String, Object> selectMap(@Language("SQL") final String sql, Object... args) {
        List<Map<String, Object>> result = selectList(sql, args);
        return DataAccessUtils.singleResult(result);
    }

    @Override
    public List<Map<String, Object>> selectList(@Language("SQL") final String sql, Object... args) {
        logger.info("sql = {}", sql);
        Optional.ofNullable(args).filter(e -> e.length > 0).map(Arrays::asList).ifPresent(e -> logger.info("args = {}", e));
        return getJdbcTemplate().queryForList(sql, args);
    }

    @Override
    public PageResult<Map<String, Object>> selectPage(@Language("SQL") final String sql, long pageNum, long pageSize, Object... args) {
        long total = count(sql, args);
        long pages = 0L;
        List<Map<String, Object>> records = Collections.emptyList();
        if (total > 0L) {
            if (pageSize > 0L) {
                pages = (total / pageSize) + (total % pageSize == 0L ? 0L : 1L);
                if (pageNum > 0L && pageNum <= pages) {
                    String pageSql = getDBType().getDialect().pageSql(sql, (pageNum - 1L) * pageSize, pageSize);
                    records = selectList(pageSql, args);
                }
            }
        }
        return PageResult.of(pageNum, pageSize, total, pages, records);
    }
}