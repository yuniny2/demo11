package com.example.demo04.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

public class DataSourceConfiguration {
    @Bean(name = "mainDataSource") @Qualifier("mainDataSource")
    public DataSource mainDataSource() {
        DriverManagerDataSource ret = new DriverManagerDataSource();
        ret.setDriverClassName("com.mysql.jdbc.Driver");
        ret.setUsername("batch");
        ret.setPassword("batch");
        ret.setUrl("jdbc:mysql://localhost:3306/batch");
        return ret;
    }
}
