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
    private Random random = new Random(System.currentTimeMillis());

    @Before
    public void before() throws SQLException, ClassNotFoundException {
        Connection conn = TSDBCommon.getConn("localhost");
        TSDBCommon.createDatabase(conn, "irp_test");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("create table weather(ts timestamp, temperature int)");
        }
        conn.close();
    }

    @Test
    public void testInvalidResultPointerException() {
        try (Connection conn = TSDBCommon.getConn("localhost")) {
            Statement stmt = conn.createStatement();

            IntStream.of(1, 2).boxed().map(i -> new Thread(() -> {
                long end = System.currentTimeMillis() + 1000 * 60 * 5;
                while (System.currentTimeMillis() < end) {
                    String sql = "insert into irp_test.weather values(now, " + random.nextInt(100) + ")";
                    try {
                        stmt.execute(sql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + " >>> " + sql);
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "Thread-" + i)).forEach(Thread::start);

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
