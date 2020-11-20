package com.taosdata.jdbc.rs;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

public class RestfulDriverTest {

    @Test
    public void testAcceptUrl() throws SQLException {
        Driver driver = new RestfulDriver();
        boolean isAccept = driver.acceptsURL("jdbc:TAOS-RS://master:6041");
        Assert.assertTrue(isAccept);
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {

    }

}
