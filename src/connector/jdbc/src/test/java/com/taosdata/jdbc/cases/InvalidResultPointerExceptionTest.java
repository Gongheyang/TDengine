package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.util.TSDBCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class InvalidResultPointerExceptionTest {


    private Connection conn;
    private Random random = new Random(System.currentTimeMillis());

    @Before
    public void before() throws SQLException, ClassNotFoundException {
        conn = TSDBCommon.getConn("localhost");
        TSDBCommon.createDatabase(conn, "irp_test");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("create table weather(ts timestamp, temperature int)");
        }
    }

    @Test
    public void testInvalidResultPointerException() {
        IntStream.of(1, 2).boxed().map(i -> new Thread(() -> {
            try (Statement stmt = conn.createStatement()) {
                long end = System.currentTimeMillis() + 1000 * 60 * 5;
                while (System.currentTimeMillis() < end) {
                    String sql = "insert into irp_test.weather values(now, " + random.nextInt(100) + ")";
                    stmt.execute(sql);
                    System.out.println(Thread.currentThread().getName() + " >>> " + sql);
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, "Thread-" + i)).forEach(Thread::start);
    }

    @After
    public void after() throws SQLException {
        if (conn != null)
            conn.close();
    }
}
