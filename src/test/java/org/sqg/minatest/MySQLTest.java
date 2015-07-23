package org.sqg.minatest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLTest {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MySQLTest.class);

    private final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/test";
    private static final Map<Long, Long> DELTAS = new HashMap<>();

    @Before
    public void setUp() throws SQLException {
        DELTAS.put(1L, 5L);
        DELTAS.put(2L, 6L);
        try (Connection connection = DriverManager
                .getConnection(CONNECTION_STRING)) {
            LOGGER.info("{} {}", connection.getMetaData().getDriverName(),
                    connection.getMetaData().getDriverVersion());
            LOGGER.info("setup tables");
            try (Statement stmt = connection.createStatement()) {
                stmt.addBatch("CREATE TABLE daily_hit(id INTEGER, impression INTEGER, last_changed TIMESTAMP) ENGINE=InnoDB");
                stmt.addBatch("INSERT daily_hit(id, impression, last_changed) VALUES(1, 0, NOW())");
                stmt.addBatch("INSERT daily_hit(id, impression, last_changed) VALUES(2, 0, NOW())");

                stmt.addBatch("CREATE TABLE total_hit(id INTEGER, impression INTEGER, last_changed TIMESTAMP) ENGINE=InnoDB");
                stmt.addBatch("INSERT total_hit(id, impression, last_changed) VALUES(1, 0, NOW())");
                stmt.addBatch("INSERT total_hit(id, impression, last_changed) VALUES(2, 0, NOW())");
                stmt.executeBatch();
            }
        }
    }

    @After
    public void tearDown() throws SQLException {
        DELTAS.clear();
        try (Connection connection = DriverManager
                .getConnection(CONNECTION_STRING)) {
            LOGGER.info("drop tables");
            try (Statement stmt = connection.createStatement()) {
                stmt.addBatch("DROP TABLE daily_hit");
                stmt.addBatch("DROP TABLE total_hit");
                stmt.executeBatch();
            }
        }
    }

    @Test
    public void testTransactionTableLock() throws SQLException,
            InterruptedException {

        Thread t1 = new Thread() {
            @Override
            public void run() {
                final String UPDATE_DAILY_HIT_SQL = "UPDATE daily_hit SET impression = impression + ? , last_changed = NOW() WHERE id = ?";
                final String UPDATE_TOTAL_HIT_SQL = "UPDATE total_hit SET impression = impression + ? , last_changed = NOW() WHERE id = ?";
                try (Connection connection = DriverManager
                        .getConnection(CONNECTION_STRING)) {
                    connection.setAutoCommit(false);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                        long t1 = System.currentTimeMillis();
                        try (PreparedStatement dailyStmt = connection
                                .prepareStatement(UPDATE_DAILY_HIT_SQL);
                                PreparedStatement totalStmt = connection
                                        .prepareStatement(UPDATE_TOTAL_HIT_SQL)) {
                            for (; System.currentTimeMillis() - t1 < TimeUnit.SECONDS
                                    .toMillis(5L);) {
                                try {
                                    for (Map.Entry<Long, Long> delta : DELTAS
                                            .entrySet()) {
                                        dailyStmt.setLong(1, delta.getValue());
                                        dailyStmt.setLong(2, delta.getKey());
                                        dailyStmt.executeUpdate();
                                        totalStmt.setLong(1, delta.getValue());
                                        totalStmt.setLong(2, delta.getKey());
                                        totalStmt.executeUpdate();
                                    }
                                    connection.commit();
                                } catch (SQLException e) {
                                    connection.rollback();
                                }
                            }
                        }
                    } catch (SQLException e) {
                        connection.rollback();
                        throw e;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                }
            }
        };
        Thread t2 = new Thread() {
            final String SELECT_DAILY_HIT_SQL = "SELECT id, impression, last_changed from daily_hit";
            final String SELECT_TOTAL_HIT_SQL = "SELECT id, impression, last_changed from total_hit";

            @Override
            public void run() {
                long id = 0L;
                long impression = 0L;
                try (Connection connection = DriverManager
                        .getConnection(CONNECTION_STRING)) {
                    TimeUnit.SECONDS.sleep(1L);
                    long t1 = System.currentTimeMillis();
                    Map<Long, Long> dailies = new HashMap<Long, Long>();
                    for (; System.currentTimeMillis() - t1 < TimeUnit.SECONDS
                            .toMillis(6L);) {
                        try (PreparedStatement stmt = connection
                                .prepareStatement(SELECT_DAILY_HIT_SQL,
                                        ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY)) {
                            try (ResultSet rs = stmt.executeQuery()) {
                                while (rs.next()) {
                                    id = rs.getLong(1);
                                    impression = rs.getLong(2);
                                    dailies.put(Long.valueOf(id),
                                            Long.valueOf(impression));
                                }
                            }
                        }
                        try (PreparedStatement stmt = connection
                                .prepareStatement(SELECT_TOTAL_HIT_SQL,
                                        ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY)) {
                            try (ResultSet rs = stmt.executeQuery()) {
                                while (rs.next()) {
                                    id = rs.getLong(1);
                                    impression = rs.getLong(2);
                                    if (impression != dailies.get(id)
                                            && impression < dailies.get(id)) {
                                        LOGGER.warn("************* ATTENTION **************");
                                        LOGGER.warn(
                                                "id = {}, daily = {}, total = {}, delta = {}",
                                                id,
                                                dailies.get(Long.valueOf(id)),
                                                impression,
                                                DELTAS.get(Long.valueOf(id)));
                                        LOGGER.warn("*************************************");
                                    }
                                }
                            }
                        }
                    }
                    LOGGER.info("id = {}, daily = {}, total = {}, delta = {}",
                            id, dailies.get(id), impression, DELTAS.get(id));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                }
            }
        };

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }

}
