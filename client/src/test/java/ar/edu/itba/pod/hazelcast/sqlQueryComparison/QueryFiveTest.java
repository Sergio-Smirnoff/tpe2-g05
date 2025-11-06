package ar.edu.itba.pod.hazelcast.sqlQueryComparison;


import ar.edu.itba.pod.hazelcast.query5.TotalMilesCollator;
import ar.edu.itba.pod.hazelcast.query5.TotalMilesCombinerFactory;
import ar.edu.itba.pod.hazelcast.query5.TotalMilesMapper;
import ar.edu.itba.pod.hazelcast.query5.TotalMilesReducerFactory;
import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesResult;
import ar.edu.itba.pod.hazelcast.query5.objects.TripRowQ5;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;


public class QueryFiveTest extends BaseQueryComparisonTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryFiveTest.class);

    @Test
    public void testQueryFive_SQLvsMapReduce() throws Exception {
        logger.info("Starting Query 5 SQL vs MapReduce comparison test...");

        List<String> sqlResults = getQueryFiveSQLResults();
        List<String> mapReduceResults = getQueryFiveMapReduceResults();

        assertFalse(sqlResults.isEmpty());
        assertFalse(mapReduceResults.isEmpty());

        Assertions.assertEquals(sqlResults.getFirst(), mapReduceResults.getFirst());

        for (int i = 1; i < sqlResults.size(); i++) {
            String sqlLine = sqlResults.get(i);
            String mrLine = mapReduceResults.get(i);

            String[] sqlParts = sqlLine.split(";");
            String[] mrParts = mrLine.split(";");

            Assertions.assertEquals(sqlParts[0], mrParts[0]);
            Assertions.assertEquals(sqlParts[1], mrParts[1]);
            Assertions.assertEquals(sqlParts[2], mrParts[2]);

            double sqlMiles = Double.parseDouble(sqlParts[3]);
            double mrMiles = Double.parseDouble(mrParts[3]);
            double tolerance = 0.1;

            Assertions.assertEquals(sqlMiles, mrMiles, tolerance);
        }

    }

    private List<String> getQueryFiveSQLResults() {
        String sqlQuery =
                """
                WITH MonthlyMiles AS (
                    SELECT
                        company,
                        YEAR(CAST(request_datetime AS TIMESTAMP)) AS trip_year,
                        MONTH(CAST(request_datetime AS TIMESTAMP)) AS trip_month,
                        SUM(CAST(trip_miles AS DOUBLE)) AS monthly_miles
                    FROM
                        trips_data
                    GROUP BY
                        company, trip_year, trip_month
                )
                SELECT
                    company,
                    trip_year,
                    trip_month,
                    TRUNCATE(SUM(monthly_miles) OVER (
                                        PARTITION BY company, trip_year
                                        ORDER BY trip_month ASC
                                        ROWS UNBOUNDED PRECEDING
                                    ), 2) AS milesYTD
                FROM
                    MonthlyMiles
                ORDER BY
                    company ASC, trip_year ASC, trip_month ASC;
                """;

        List<String> results = new ArrayList<>();
        results.add("company;year;month;milesYTD");
        logger.info("Executing SQL query for validation...");

        try (Statement stmt = h2Connection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {

            while (rs.next()) {
                String company = rs.getString("company");
                int year = rs.getInt("trip_year");
                int month = rs.getInt("trip_month");
                double milesYTD = rs.getDouble("milesYTD");

                String resultLine = String.format("%s;%d;%d;%.2f", company, year, month, milesYTD);
                results.add(resultLine);
            }

        } catch (SQLException e) {
            fail("SQL query execution failed: " + e.getMessage());
        }

        logger.info("SQL query finished successfully. Found {} results.", results.size() - 1);
        return results;
    }

    private List<String> getQueryFiveMapReduceResults() throws Exception {
        logger.info("Starting MapReduce job for Query 5 test...");

        IMap<Integer, TripRowQ5> tripsMap = hazelcastInstance.getMap("trips-q5");

        URL tripsUrl = QueryFiveTest.class.getResource(TRIPS_RESOURCE_PATH);
        if (tripsUrl == null) {
            fail("Test resource CSV file not found: " + TRIPS_RESOURCE_PATH);
        }
        String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();

        Predicate<String[]> filter = line -> true;

        Function<String[], TripRowQ5> mapper = line -> new TripRowQ5(
                line[0],
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                Double.parseDouble(line[6])
        );

        logger.info("Loading trips data for MapReduce...");
        KeyValueSource<Integer, TripRowQ5> tripsKeyValueSource = loadTripsData(filter, mapper, tripsMap, tripsPath);
        logger.info("Finished loading trips map (size: {})", tripsMap.size());

        JobTracker jobTracker = hazelcastInstance.getJobTracker("test-query-5");


        ICompletableFuture<List<TotalMilesResult>> job = jobTracker.newJob(tripsKeyValueSource)
                .mapper(new TotalMilesMapper())
                .combiner(new TotalMilesCombinerFactory())
                .reducer(new TotalMilesReducerFactory())
                .submit(new TotalMilesCollator()); // Collator computes YTD and sorts

        List<TotalMilesResult> results = job.get();
        logger.info("Finished map reduce job");

        List<String> toReturn = new ArrayList<>();
        toReturn.add("company;year;month;milesYTD");
        for (TotalMilesResult result : results) {
            toReturn.add(result.toString());
        }

        return toReturn;
    }

    private KeyValueSource<Integer, TripRowQ5> loadTripsData(
            Predicate<? super String[]> filter,
            Function<String[], TripRowQ5> mapper,
            IMap<Integer, TripRowQ5> tripsMap,
            String tripsPath) throws IOException {

        tripsMap.clear();
        KeyValueSource<Integer, TripRowQ5> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
        final AtomicInteger tripsMapKey = new AtomicInteger();

        try (Stream<String> lines = Files.lines(Paths.get(tripsPath), StandardCharsets.UTF_8)) {
            lines.parallel().skip(1)
                    .map(line -> line.split(";"))
                    .filter(filter)
                    .map(mapper)
                    .forEach(trip -> {
                        Integer uniqueId = tripsMapKey.getAndIncrement();
                        tripsMap.put(uniqueId, trip);
                    });
        }
        return tripsKeyValueSource;
    }
}