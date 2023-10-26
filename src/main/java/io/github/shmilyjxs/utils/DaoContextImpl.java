package io.github.shmilyjxs.utils;

import io.github.shmilyjxs.dialects.DBType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

import javax.sql.DataSource;
import java.util.Objects;

public class DaoContextImpl extends BeanDaoContext {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final LazyInitializer<DBType> lazyDBType;

    public DaoContextImpl(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.lazyDBType = new LazyInitializer<DBType>() {
            @Override
            protected DBType initialize() {
                try {
                    return JdbcUtils.extractDatabaseMetaData(dataSource, e -> DBType.fromProductName(e.getDatabaseProductName()));
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
    public DBType getDBType() {
        return Objects.requireNonNull(lazyDBType.get());
    }
}