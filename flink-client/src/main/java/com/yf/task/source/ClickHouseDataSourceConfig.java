package com.yf.task.source;

import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariConfig;
import com.ververica.cdc.connectors.shaded.com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@ComponentScan(basePackages = "com.yf.service")
@PropertySource("classpath:config.properties")
public class ClickHouseDataSourceConfig {

    @Bean
    public DataSource dataSource(Environment env) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(env.getProperty("clickhouse.url"));
        config.setUsername(env.getProperty("clickhouse.user"));
        config.setPassword(env.getProperty("clickhouse.password"));
        config.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");
        config.setMaximumPoolSize(Integer.parseInt(env.getProperty("clickhouse.maximumPoolSize", "10")));
        config.setMinimumIdle(Integer.parseInt(env.getProperty("clickhouse.minimumIdle", "2")));
        return new HikariDataSource(config);
    }
}
