package ar.edu.itba.pod.hazelcast.sqlQueryComparison;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query4.*;
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

public class QueryFourTest extends BaseQueryComparisonTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryFourTest.class);
    private static final String borough = "Manhattan";

    @Test
    public void testQueryFour_SQLvsMapReduce() throws Exception {
        logger.info("Starting Query 4 SQL vs MapReduce comparison test...");

        List<String> sqlResults = getQueryFourSQLResults();
        List<String> mapReduceResults = getQueryFourMapReduceResults();

        assertFalse(sqlResults.isEmpty());
        assertFalse(mapReduceResults.isEmpty());

        Assertions.assertEquals(sqlResults, mapReduceResults);
    }

    private List<String> getQueryFourSQLResults() {
        String sqlQuery =
                """
                WITH RankedDelays AS (
                    SELECT
                        t.PULocationID,
                        t.DOLocationID,
                        DATEDIFF('SECOND', CAST(t.request_datetime AS TIMESTAMP), CAST(t.pickup_datetime AS TIMESTAMP)) AS delay_in_seconds,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.PULocationID
                            ORDER BY
                                DATEDIFF('SECOND', CAST(t.request_datetime AS TIMESTAMP), CAST(t.pickup_datetime AS TIMESTAMP)) DESC,
                                z_drop.Zone ASC
                        ) AS rnk
                    FROM
                        trips_data t
                    JOIN
                        zones_data z_pick ON t.PULocationID = z_pick.LocationID
                    JOIN
                        zones_data z_drop ON t.DOLocationID = z_drop.LocationID
                    WHERE
                        z_pick.Borough = 'Manhattan'
                )
                SELECT
                    z_pick.Zone AS pickUpZone,
                    z_drop.Zone AS dropOffZone,
                    rd.delay_in_seconds AS delayInSeconds
                FROM
                    RankedDelays rd
                JOIN
                    zones_data z_pick ON rd.PULocationID = z_pick.LocationID
                JOIN
                    zones_data z_drop ON rd.DOLocationID = z_drop.LocationID
                WHERE
                    rd.rnk = 1
                ORDER BY
                    pickUpZone ASC;
                """;

        List<String> results = new ArrayList<>();
        results.add("pickUpZone;dropOffZone;delayInSeconds");
        logger.info("Executing SQL query for validation...");

        try (Statement stmt = h2Connection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {

            while (rs.next()) {
                String startZone = rs.getString("pickUpZone");
                String endZone = rs.getString("dropOffZone");
                long delay = rs.getLong("delayInSeconds"); // Use delay

                String resultLine = String.format("%s;%s;%d", startZone, endZone, delay);
                results.add(resultLine);
            }

        } catch (SQLException e) {
            fail("SQL query execution failed: " + e.getMessage());
        }

        logger.info("SQL query finished successfully. Found {} results.", results.size() - 1);
        return results;
    }

    private List<String> getQueryFourMapReduceResults() throws Exception {
        logger.info("Starting MapReduce job for Query 4 test...");

        IMap<Integer, TripRowQ4> tripsMap = hazelcastInstance.getMap("trips-q4");


        URL tripsUrl = QueryFourTest.class.getResource(TRIPS_RESOURCE_PATH);
        if (tripsUrl == null) {
            fail("Test resource CSV file not found: " + TRIPS_RESOURCE_PATH);
        }
        String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();

        Predicate<String[]> filter = line ->{
            ZonesRow PUZoneRow = zonesMap.get(Integer.parseInt(line[4]));
            ZonesRow DOZoneRow = zonesMap.get(Integer.parseInt(line[5]));

            return PUZoneRow != null
                    && DOZoneRow != null
                    && PUZoneRow.getBorough().equals(borough);
        };

        Function<String[], TripRowQ4> mapper = line -> new TripRowQ4(
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                LocalDateTime.parse(line[2], DATE_TIME_FORMATTER),
                zonesMap.get(Integer.parseInt(line[4])).getZone(),
                zonesMap.get(Integer.parseInt(line[5])).getZone()
        );

        logger.info("Loading trips data for MapReduce...");
        KeyValueSource<Integer, TripRowQ4> tripsKeyValueSource = loadTripsData(filter, mapper, tripsMap, tripsPath);
        logger.info("Finished loading trips map (size: {})", tripsMap.size());

        JobTracker jobTracker = hazelcastInstance.getJobTracker("test-query-4");

        ICompletableFuture<SortedSet<QueryOneFourResult>> job = jobTracker.newJob(tripsKeyValueSource)
                .mapper(new DelayPerBoroughZoneMapper())
                .combiner(new DelayPerBoroughZoneCombinerFactory())
                .reducer(new DelayPerBoroughZoneReducerFactory())
                .submit(new DelayPerBoroughZoneCollator());

        SortedSet<QueryOneFourResult> results = job.get();
        logger.info("Finished map reduce job");

        List<String> toReturn = new ArrayList<>();
        toReturn.add("pickUpZone;dropOffZone;delayInSeconds");
        for (QueryOneFourResult result : results) {
            toReturn.add(result.toString());
        }

        return toReturn;
    }

    private KeyValueSource<Integer, TripRowQ4> loadTripsData(
            Predicate<? super String[]> filter,
            Function<String[], TripRowQ4> mapper,
            IMap<Integer, TripRowQ4> tripsMap,
            String tripsPath) throws IOException {

        tripsMap.clear();
        KeyValueSource<Integer, TripRowQ4> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
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