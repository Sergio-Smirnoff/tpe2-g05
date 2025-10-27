package ar.edu.itba.pod.hazelcast.client.queryOne;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class QueryOneTest {
    private static Connection h2Connection;
    private static final Logger logger = LoggerFactory.getLogger(QueryOneTest.class);

    @BeforeAll
    public static void setUp() {
        String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
        logger.info("Initiating H2 in-memory database connection.");
        try {
            h2Connection = DriverManager.getConnection(jdbcUrl);
            logger.info("Connected to H2 database.");

            try (Statement stmt = h2Connection.createStatement()) {

                URL tripsUrl = QueryOneTest.class.getResource("/trips-2025-01-mini.csv");
                URL zonesUrl = QueryOneTest.class.getResource("/zones.csv");

                if (tripsUrl == null || zonesUrl == null) {
                    fail("Test resource CSV files not found in classpath. Make sure they are in src/test/resources.");
                }

                String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();
                String zonesPath = new File(zonesUrl.toURI()).getAbsolutePath();

                String sql1 = String.format("CREATE TABLE trips_data AS SELECT * FROM CSVREAD('%s', null, 'fieldSeparator=;');", tripsPath);
                String sql2 = String.format("CREATE TABLE zones_data AS SELECT * FROM CSVREAD('%s', null, 'fieldSeparator=;');", zonesPath);

                stmt.execute(sql1);
                stmt.execute(sql2);
                logger.info("Data loaded into H2 from CSV files.");
            }
        } catch (SQLException | URISyntaxException e) {
            fail("Database setup failed: " + e.getMessage());
        }
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        if (h2Connection != null && !h2Connection.isClosed()) {
            h2Connection.close();
            logger.info("Closed connection to H2 database.");
        }
    }

    @Test
    public void testQueryOneSQL() {
        String sqlQuery =
                "SELECT " +
                        "    pz.ZONE AS startZoneName, " +
                        "    dz.ZONE AS endZoneName, " +
                        "    COUNT(*) AS trip_count " +
                        "FROM " +
                        "    trips_data t " +
                        "JOIN " +
                        "    zones_data pz ON t.PULOCATIONID = pz.LOCATIONID " +
                        "JOIN " +
                        "    zones_data dz ON t.DOLOCATIONID = dz.LOCATIONID " +
                        "WHERE " +
                        "    t.PULOCATIONID <> t.DOLOCATIONID " +
                        "GROUP BY " +
                        "    startZoneName, endZoneName " +
                        "ORDER BY " +
                        "    trip_count DESC, startZoneName ASC, endZoneName ASC";

        List<String> results = new ArrayList<>();
        logger.info("Executing SQL query for validation...");

        try (Statement stmt = h2Connection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {

            while (rs.next()) {
                String startZone = rs.getString("startZoneName");
                String endZone = rs.getString("endZoneName");
                long count = rs.getLong("trip_count");

                String resultLine = String.format("%s;%s;%d", startZone, endZone, count);
                results.add(resultLine);
            }

        } catch (SQLException e) {
            fail("SQL query execution failed: " + e.getMessage());
        }

        assertFalse(results.isEmpty(), "The SQL query should return at least one result.");

        logger.info("SQL query finished successfully. Found {} unique routes.", results.size());
        results.stream().limit(10).forEach(logger::info);
    }
}