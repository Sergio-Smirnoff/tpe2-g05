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

public class QueryFourTest {
    private static Connection h2Connection;
    private static final Logger logger = LoggerFactory.getLogger(QueryFourTest.class);

    @BeforeAll
    public static void setUp() {
        String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
        logger.info("Initiating H2 in-memory database connection.");
        try {
            h2Connection = DriverManager.getConnection(jdbcUrl);
            logger.info("Connected to H2 database.");

            try (Statement stmt = h2Connection.createStatement()) {

                String tripsResourcePath = "/trips-2025-01-mini.csv";
                URL tripsUrl = QueryFourTest.class.getResource(tripsResourcePath);
                URL zonesUrl = QueryFourTest.class.getResource("/zones.csv");

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
                """

                        SELECT\s
    z_pick.Zone AS pickUpZone,
    z_drop.Zone AS dropOffZone,
    t.delay_in_seconds AS delayInSeconds
FROM (
    SELECT\s
        PULocationID,
        DOLocationID,
        DATEDIFF('SECOND', request_datetime, pickup_datetime) AS delay_in_seconds,
        RANK() OVER (
            PARTITION BY PULocationID
            ORDER BY\s
                DATEDIFF('SECOND', request_datetime, pickup_datetime) DESC,
                DOLocationID ASC
        ) AS rnk
    FROM trips_data
) t
JOIN zones_data z_pick ON t.PULocationID = z_pick.LocationID
JOIN zones_data z_drop ON t.DOLocationID = z_drop.LocationID
WHERE z_pick.Borough = 'Manhattan'
  AND t.rnk = 1
ORDER BY z_pick.Zone ASC;

""";

        List<String> results = new ArrayList<>();
        logger.info("Executing SQL query for validation...");

        try (Statement stmt = h2Connection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {

            while (rs.next()) {
                String startZone = rs.getString("pickUpZone");
                String endZone = rs.getString("dropOffZone");
                long count = rs.getLong("delayInSeconds");

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