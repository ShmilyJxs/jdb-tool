package io.github.shmilyjxs.utils;

import io.github.shmilyjxs.dialects.DBEnum;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;

public class DaoContextImpl extends BeanDaoContext {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final DBEnum dbEnum;

    public DaoContextImpl(DataSource dataSource) throws Exception {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        Connection connection = dataSource.getConnection();
        try {
            String dbType = connection.getMetaData().getDatabaseProductName();
            this.dbEnum = DBEnum.fromProductName(dbType);
        } finally {
            connection.close();
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    @Override
    public NamedParameterJdbcTemplate getNamedJdbcTemplate() {
        return namedJdbcTemplate;
    }

    @Override
    public DBEnum getDBEnum() {
        return dbEnum;
    }
}