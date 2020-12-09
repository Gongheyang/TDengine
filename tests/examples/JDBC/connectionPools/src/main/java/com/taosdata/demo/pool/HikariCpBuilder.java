package com.taosdata.demo.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class HikariCpBuilder {

    public static DataSource getDataSource(String host, int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        config.setJdbcUrl("jdbc:TAOS://" + host + ":6030");
        config.setUsername("root");
        config.setPassword("taosdata");

//        config.setMaximumPoolSize(poolSize);
//        config.setMinimumIdle(poolSize);
        config.setMaximumPoolSize(200);
        config.setMinimumIdle(50);
        config.setConnectionTestQuery("show dnodes");

        config.setIdleTimeout(60000);
        config.setConnectionTimeout(60000);
        config.setValidationTimeout(3000);
        config.setMaxLifetime(60000);

        HikariDataSource ds = new HikariDataSource(config);
        return ds;
    }
}
