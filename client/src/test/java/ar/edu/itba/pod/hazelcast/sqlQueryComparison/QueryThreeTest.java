package ar.edu.itba.pod.hazelcast.sqlQueryComparison;


import ar.edu.itba.pod.hazelcast.query3.*;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;


public class QueryThreeTest extends BaseQueryComparisonTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryThreeTest.class);

    @Test
    public void testQueryThree_SQLvsMapReduce() throws Exception {
        logger.info("Starting Query 3 SQL vs MapReduce comparison test...");

        List<String> sqlResults = getQueryThreeSQLResults();
        List<String> mapReduceResults = getQueryThreeMapReduceResults();

        assertFalse(sqlResults.isEmpty());
        assertFalse(mapReduceResults.isEmpty());

        Assertions.assertEquals(sqlResults, mapReduceResults);
    }

    private List<String> getQueryThreeSQLResults() {
        String sqlQuery =
                """
                SELECT
                    z.Borough AS pickUpBorough,
                    t.company,
                    AVG(CAST(t.base_passenger_fare AS DOUBLE)) AS avgFare
                FROM
                    trips_data t
                JOIN
                    zones_data z ON t.PULocationID = z.LocationID
                WHERE
                    t.PULocationID <> 265
                GROUP BY
                    z.Borough, t.company
                ORDER BY
                    avgFare DESC, pickUpBorough ASC, company ASC;
                """;

        List<String> results = new ArrayList<>();
        results.add("pickUpBorough;company;avgFare");
        logger.info("Executing SQL query for validation...");

        try (Statement stmt = h2Connection.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {

            while (rs.next()) {
                String pickUpBorough = rs.getString("pickUpBorough");
                String company = rs.getString("company");
                double avgFare = rs.getDouble("avgFare");

                String resultLine = String.format("%s;%s;%.2f", pickUpBorough, company, avgFare);
                results.add(resultLine);
            }

        } catch (SQLException e) {
            fail("SQL query execution failed: " + e.getMessage());
        }

        logger.info("SQL query finished successfully. Found {} results.", results.size() - 1);
        return results;
    }

    private List<String> getQueryThreeMapReduceResults() throws Exception {
        logger.info("Starting MapReduce job for Query 3 test...");


        IMap<Integer, TripRowQ3> tripsMap = hazelcastInstance.getMap("trips-q3");

        URL tripsUrl = QueryThreeTest.class.getResource(TRIPS_RESOURCE_PATH);
        if (tripsUrl == null) {
            fail("Test resource CSV file not found: " + TRIPS_RESOURCE_PATH);
        }
        String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();

        Predicate<String[]> filter = line -> {
            int puId = Integer.parseInt(line[4]);

            return (puId != OUTSIDE_NYC_ID) &&
                    (zonesMap.get(Integer.parseInt(line[4])) != null);
        };

        Function<String[], TripRowQ3> mapper = line -> new TripRowQ3(
                zonesMap.get(Integer.parseInt(line[4])).getBorough(), // borough
                line[0],
                Double.parseDouble(line[7])
        );

        logger.info("Loading trips data for MapReduce...");
        KeyValueSource<Integer, TripRowQ3> tripsKeyValueSource = loadTripsData(filter, mapper, tripsMap, tripsPath);
        logger.info("Finished loading trips map (size: {})", tripsMap.size());

        JobTracker jobTracker = hazelcastInstance.getJobTracker("test-query-3");

        ICompletableFuture<List<AvgPriceBoroughCompany>> job = jobTracker.newJob(tripsKeyValueSource)
                .mapper(new PriceAvgMapper())
                .reducer(new PickupCompanyPairReducerFactory())
                .submit(new Query3Collator());

        List<AvgPriceBoroughCompany> results = job.get();
        logger.info("Finished map reduce job");

        List<String> toReturn = new ArrayList<>();
        toReturn.add("pickUpBorough;company;avgFare");
        for (AvgPriceBoroughCompany result : results) {
            toReturn.add(result.toString());
        }

        return toReturn;
    }

    private KeyValueSource<Integer, TripRowQ3> loadTripsData(
            Predicate<? super String[]> filter,
            Function<String[], TripRowQ3> mapper,
            IMap<Integer, TripRowQ3> tripsMap,
            String tripsPath) throws IOException {

        tripsMap.clear();
        KeyValueSource<Integer, TripRowQ3> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
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