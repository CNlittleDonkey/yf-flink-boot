package com.yf.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Service
public class ClickHouseDataService {
    private final DataSource dataSource;

    @Autowired
    public ClickHouseDataService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insertData(String paramSn, String paramValue) throws SQLException {
        String sql = "INSERT INTO ibtes_cloud.my_table (param_sn, param_value) VALUES (?, ?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, paramSn);
            preparedStatement.setString(2, paramValue);
            preparedStatement.executeUpdate();
        }
    }
}
