package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.util.RestfulJDBCCommon;
import org.junit.Test;

import java.sql.*;

public class RestfulJdbcTest {

    @Test
    public void testCase001() {
        try (Connection conn = RestfulJDBCCommon.getConnection("master")) {

            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from log.log");
            ResultSetMetaData metaData = resultSet.getMetaData();
            int total = 0;
            while (resultSet.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String column = metaData.getColumnLabel(i);
                    String value = resultSet.getString(i);
                    System.out.print(column + ":" + value + "\t");
                }
                total++;
                System.out.println();
            }
            System.out.println("total : " + total);
            statement.close();

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCase002() {
        try (Connection conn = RestfulJDBCCommon.getConnection("master")) {
            RestfulJDBCCommon.createDatabase(conn, "restful_test");
            RestfulJDBCCommon.createSuperTable(conn);


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
