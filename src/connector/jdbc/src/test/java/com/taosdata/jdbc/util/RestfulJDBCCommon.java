package com.taosdata.jdbc.util;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class RestfulJDBCCommon {

    public static Connection getConnection(String host) throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        return DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata");
    }

    public static void createDatabase(Connection conn, String dbName) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database if not exists " + dbName);
        stmt.execute("use " + dbName);
        stmt.close();
    }

    public static void createSuperTable(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        Map<String, String> fieldsMeta = new LinkedHashMap<>();
        fieldsMeta.put("ts", "timestamp");
        fieldsMeta.put("temperature", "float");
        fieldsMeta.put("humidity", "int");
        Map<String, String> tagsMeta = new LinkedHashMap<>();
        tagsMeta.put("location", "nchar");
        tagsMeta.put("tableId", "int");
        String sql = SqlSpeller.createSuperTable("weather", fieldsMeta, tagsMeta);
        stmt.executeUpdate(sql);
        stmt.close();
    }


}
