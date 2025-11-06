package ar.edu.itba.pod.hazelcast.sqlQueryComparison;

import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query1.*;

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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;


public class QueryOneTest extends BaseQueryComparisonTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryOneTest.class);

    @Test
    public void testQueryOne_SQLvsMapReduce() throws Exception {
        logger.info("Starting SQL vs MapReduce comparison test...");

        List<String> sqlResults = getQueryOneSQLResults();
        List<String> mapReduceResults = getQueryOneMapReduceResults();

        assertFalse(sqlResults.isEmpty());
        assertFalse(mapReduceResults.isEmpty());

        Assertions.assertEquals(sqlResults, mapReduceResults);
    }


    private List<String> getQueryOneSQLResults() {
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
        results.add("pickUpZone;dropOffZone;trips");
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

        logger.info("SQL query finished successfully. Found {} unique routes.", results.size() - 1);
        return results;
    }

    private List<String> getQueryOneMapReduceResults() throws Exception {
        logger.info("Starting MapReduce job for Query 1 test...");

        IMap<Integer, TripRowQ1> tripsMap = hazelcastInstance.getMap("trips-q1");

        URL tripsUrl = QueryOneTest.class.getResource(TRIPS_RESOURCE_PATH);
        if (tripsUrl == null) {
            fail("Test resource CSV file not found: " + TRIPS_RESOURCE_PATH);
        }
        String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();


        Predicate<String[]> filter = line -> {
            int puId = Integer.parseInt(line[4]);
            int doId = Integer.parseInt(line[5]);

            return (puId != doId) &&
                    (zonesMap.containsKey(puId)) &&
                    (zonesMap.containsKey(doId));

        };

        Function<String[], TripRowQ1> mapper = line -> new TripRowQ1(
                zonesMap.get(Integer.parseInt(line[4])).getZone(),
                zonesMap.get(Integer.parseInt(line[5])).getZone()
        );

        logger.info("Loading trips data for MapReduce...");
        KeyValueSource<Integer, TripRowQ1> tripsKeyValueSource = loadTripsData(filter, mapper, tripsMap, tripsPath);
        logger.info("Finished loading trips map (size: {})", tripsMap.size());

        JobTracker jobTracker = hazelcastInstance.getJobTracker("test-query-1");

        ICompletableFuture<SortedSet<QueryOneFourResult>> job = jobTracker.newJob(tripsKeyValueSource)
                .mapper(new StartEndPairMapper())
                .combiner(new StartEndPairCombinerFactory())
                .reducer(new StartEndPairReducerFactory())
                .submit(new QueryOneCollator());

        SortedSet<QueryOneFourResult> results = job.get();

        logger.info("Finished map reduce job");

        List<String> toReturn = new ArrayList<>();
        toReturn.add("pickUpZone;dropOffZone;trips");
        for (QueryOneFourResult result : results) {
            toReturn.add(result.toString());
        }

        return toReturn;
    }

    private KeyValueSource<Integer, TripRowQ1> loadTripsData(
            Predicate<? super String[]> filter,
            Function<String[], TripRowQ1> mapper,
            IMap<Integer, TripRowQ1> tripsMap,
            String tripsPath) throws IOException {

        tripsMap.clear();
        KeyValueSource<Integer, TripRowQ1> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
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