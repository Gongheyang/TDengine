package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.lib.TSDBCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PreparedStatementTest {
    private static String host = "localhost";

    private Connection conn;

    @Before
    public void before() {
        try {
            conn = TSDBCommon.getConn(host);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateSuperTable() {
        try {
            String sql = "create table if not exists ? (ts timestamp, ? float, ? int) tags(?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "td");
            pstmt.setString(2, "temperature");
            pstmt.setString(3, "humidity");
            pstmt.setString(4, "location");
            pstmt.setString(5, "groupId");
            pstmt.executeUpdate();

            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
