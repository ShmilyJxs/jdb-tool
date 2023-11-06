package io.github.shmilyjxs.core;

import io.github.shmilyjxs.dialects.DBType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

public interface IDaoContext extends INativeDao, ISqlDao, IBeanDao {

    DataSource getDataSource();

    JdbcTemplate getJdbcTemplate();

    NamedParameterJdbcTemplate getNamedJdbcTemplate();

    DBType getDBType();

    Object idGenerator();
}