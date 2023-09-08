package io.github.shmilyjxs.utils;

import io.github.shmilyjxs.dialects.DBEnum;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Objects;

public class DaoContextImpl extends BeanDaoContext {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final LazyInitializer<DBEnum> lazyDBEnum;

    public DaoContextImpl(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.lazyDBEnum = new LazyInitializer<DBEnum>() {
            @Override
            protected DBEnum initialize() {
                try {
                    Connection connection = dataSource.getConnection();
                    try {
                        String dbType = connection.getMetaData().getDatabaseProductName();
                        return DBEnum.fromProductName(dbType);
                    } finally {
                        JdbcUtils.closeConnection(connection);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
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
        return Objects.requireNonNull(lazyDBEnum.get());
    }
}