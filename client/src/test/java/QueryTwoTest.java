//package ar.edu.itba.pod.hazelcast.client.queryTwo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;  
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;  
import java.nio.file.Files;  
import java.nio.file.Path;  
import java.nio.file.Paths;  
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class QueryTwoTest {
    private static Connection h2Connection;
    private static final Logger logger = LoggerFactory.getLogger(QueryTwoTest.class);

    @BeforeAll
    public static void setUp() {
        String jdbcUrl = "jdbc:h2:mem:testdb";
        logger.info("Initiating H2 in-memory database connection.");
        try {
            h2Connection = DriverManager.getConnection(jdbcUrl);
            logger.info("Connected to H2 database.");

            try (Statement stmt = h2Connection.createStatement()) {

                String tripsResourcePath = "/trips-2025-01-mini.csv";
                URL tripsUrl = QueryTwoTest.class.getResource(tripsResourcePath);
                URL zonesUrl = QueryTwoTest.class.getResource("/zones.csv");

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
    public void testQueryTwoSQL() {
        String sqlQuery =
                """
                WITH RankedTrips AS (
                    SELECT
                        z_pick.Zone AS pickUpZone,
                        z_drop.Zone AS dropOffZone,
                        t.request_datetime,
                        CAST(t.trip_miles AS DOUBLE) AS trip_miles,
                        t.company,
                        ROW_NUMBER() OVER(
                            PARTITION BY z_pick.Zone
                            ORDER BY
                                CAST(t.trip_miles AS DOUBLE) DESC,
                                CAST(t.request_datetime AS TIMESTAMP) DESC
                        ) as rnk
                    FROM
                        trips_data t
                    JOIN
                        zones_data z_pick ON t.PULocationID = z_pick.LocationID
                    JOIN
                        zones_data z_drop ON t.DOLocationID = z_drop.LocationID
                    WHERE
                        t.PULocationID <> 265 AND t.DOLocationID <> 265
                )
                SELECT
                    pickUpZone,
                    dropOffZone,
                    request_datetime,
                    trip_miles,
                    company
                FROM
                    RankedTrips
                WHERE
                    rnk = 1
                ORDER BY
                    pickUpZone ASC;
                """;

        List<String> results = new ArrayList<>();
        results.add("pickUpZone;longestDOZone;longestPUDateTime;longestMiles;longestCompany");
        logger.info("Executing SQL query for validation...");

        try (Statement stmt = h2Connection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {

            while (rs.next()) {
                String pickUpZone = rs.getString("pickUpZone");
                String dropOffZone = rs.getString("dropOffZone");
                String requestTime = rs.getString("request_datetime");
                double longestMiles = rs.getDouble("trip_miles");
                String company = rs.getString("company");

                String resultLine = String.format("%s;%s;%s;%.2f;%s",
                        pickUpZone, dropOffZone, requestTime, longestMiles, company);
                results.add(resultLine);
            }

        } catch (SQLException e) {
            fail("SQL query execution failed: " + e.getMessage());
        }

        assertFalse(results.isEmpty(), "The SQL query should return at least one result.");

        logger.info("SQL query finished successfully. Found {} results.", results.size() - 1);
        results.stream().limit(10).forEach(logger::info);

        try {
            Path outputPath = Paths.get("target/query2-sql-validation.csv");
            Files.createDirectories(outputPath.getParent());
            Files.write(outputPath, results, StandardCharsets.UTF_8);
            logger.info("Successfully wrote SQL results to: " + outputPath.toAbsolutePath());
        } catch (IOException e) {
            fail("Failed to write results to file: " + e.getMessage());
        }
    }
}