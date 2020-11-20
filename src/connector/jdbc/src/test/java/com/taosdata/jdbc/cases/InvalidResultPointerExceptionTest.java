package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.util.TSDBCommon;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
    public void testUseSameConnection() {
        try (Connection conn = TSDBCommon.getConn("localhost")) {

            List<Thread> threads = IntStream.range(0, 2).boxed().map(i -> new Thread(() -> {
                while (true) {
                    String sql = "insert into irp_test.weather values(now, " + random.nextInt(100) + ")";
                    try {
                        Statement stmt = conn.createStatement();
                        stmt.executeUpdate(sql);
                        stmt.close();
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
            }, "Thread-" + i)).collect(Collectors.toList());

            for (Thread thread : threads)
                thread.start();
            for (Thread thread : threads) {
                thread.join();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUseSameStatement() {
        try (Connection conn = TSDBCommon.getConn("localhost")) {
            Statement stmt = conn.createStatement();

            List<Thread> threads = IntStream.range(0, 2).boxed().map(i -> new Thread(() -> {
                while (true) {
                    String sql = "insert into irp_test.weather values(now, " + random.nextInt(100) + ")";
                    try {
                        stmt.executeUpdate(sql);
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
            }, "Thread-" + i)).collect(Collectors.toList());

            for (Thread thread : threads)
                thread.start();
            for (Thread thread : threads) {
                thread.join();
            }

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
