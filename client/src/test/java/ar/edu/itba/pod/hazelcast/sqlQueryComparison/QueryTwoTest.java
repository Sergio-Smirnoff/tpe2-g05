package ar.edu.itba.pod.hazelcast.sqlQueryComparison;

// TODO: Import your specific Query 2 classes (Mapper, Reducer, Collator, Row)
// e.g., import ar.edu.itba.pod.hazelcast.query2.*;
// e.g., import ar.edu.itba.pod.hazelcast.common.TripRowQ2;

import ar.edu.itba.pod.hazelcast.query2.*;
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
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

// TODO: Placeholder imports for your MR classes
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.Collator;
import java.util.Map;


public class QueryTwoTest extends BaseQueryComparisonTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryTwoTest.class);

    @Test
    public void testQueryTwo_SQLvsMapReduce() throws Exception {
        logger.info("Starting Query 2 SQL vs MapReduce comparison test...");

        List<String> sqlResults = getQueryTwoSQLResults();
        List<String> mapReduceResults = getQueryTwoMapReduceResults();

        assertFalse(sqlResults.isEmpty());
        assertFalse(mapReduceResults.isEmpty());

        Assertions.assertEquals(sqlResults, mapReduceResults);
    }

    private List<String> getQueryTwoSQLResults() {
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
                    FORMATDATETIME(CAST(request_datetime AS TIMESTAMP), 'dd/MM/yyyy HH:mm:ss') AS request_datetime_formatted,
                    TRUNCATE(trip_miles, 2) AS trip_miles_truncated,
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
                String requestTime = rs.getString("request_datetime_formatted");
                double longestMiles = rs.getDouble("trip_miles_truncated");
                String company = rs.getString("company");

                String resultLine = String.format("%s;%s;%s;%.2f;%s",
                        pickUpZone, dropOffZone, requestTime, longestMiles, company);
                results.add(resultLine);
            }

        } catch (SQLException e) {
            fail("SQL query execution failed: " + e.getMessage());
        }

        logger.info("SQL query finished successfully. Found {} results.", results.size() - 1);
        return results;
    }

    private List<String> getQueryTwoMapReduceResults() throws Exception {
        logger.info("Starting MapReduce job for Query 2 test...");

        IMap<Integer, TripRowQ2> tripsMap = hazelcastInstance.getMap("trips-q2");


        URL tripsUrl = QueryTwoTest.class.getResource(TRIPS_RESOURCE_PATH);
        if (tripsUrl == null) {
            fail("Test resource CSV file not found: " + TRIPS_RESOURCE_PATH);
        }
        String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();

        Predicate<String[]> filter = line -> {
            int puId = Integer.parseInt(line[4]);
            int doId = Integer.parseInt(line[5]);
            return (puId != OUTSIDE_NYC_ID) &&
                    (doId != OUTSIDE_NYC_ID) &&
                    (zonesMap.get(puId) != null) &&
                    (zonesMap.get(doId) != null);
        };

        Function<String[], TripRowQ2> mapper = line -> new TripRowQ2(
                zonesMap.get(Integer.parseInt(line[4])).getZone(),
                zonesMap.get(Integer.parseInt(line[5])).getZone(),
                Double.parseDouble(line[6]),
                line[0],
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER)
        );

        logger.info("Loading trips data for MapReduce...");
        KeyValueSource<Integer, TripRowQ2> tripsKeyValueSource = loadTripsData(filter, mapper, tripsMap, tripsPath);
        logger.info("Finished loading trips map (size: {})", tripsMap.size());

        JobTracker jobTracker = hazelcastInstance.getJobTracker("test-query-2");

        ICompletableFuture<SortedSet<LongestTripResult>> job = jobTracker.newJob(tripsKeyValueSource)
                .mapper(new LongestTripMapper())
                .combiner(new LongestTripCombinerFactory())
                .reducer(new LongestTripReducerFactory())
                .submit(new LongestTripCollator());

        SortedSet<LongestTripResult> results = job.get();
        logger.info("Finished map reduce job");

        List<String> toReturn = new ArrayList<>();
        toReturn.add("pickUpZone;longestDOZone;longestPUDateTime;longestMiles;longestCompany");
        for(LongestTripResult result : results) {
            toReturn.add(result.toString());
        }

        return toReturn;
    }

    private KeyValueSource<Integer, TripRowQ2> loadTripsData(
            Predicate<? super String[]> filter,
            Function<String[], TripRowQ2> mapper,
            IMap<Integer, TripRowQ2> tripsMap,
            String tripsPath) throws IOException {

        tripsMap.clear();
        KeyValueSource<Integer, TripRowQ2> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
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