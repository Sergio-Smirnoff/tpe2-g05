package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.query1.TripRowQ1;
import ar.edu.itba.pod.hazelcast.query2.*;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ClientQuery2 extends Client<TripRowQ2, LongestTripResult> {
    private static final Integer QUERY_NUMBER = 2;

    private static final int OUTSIDE_NYC_ID = 265; // ID de "Outside of NYC"

    public ClientQuery2(){
        super(QUERY_NUMBER);
    }

    @Override
    String getCsvHeader() {
        return "pickUpZone;longestDOZone;longestPUDateTime;longestMiles;longestCompany";
    }

    @Override
    ICompletableFuture<SortedSet<LongestTripResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ2> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new LongestTripMapper())
                .combiner(new LongestTripCombinerFactory())
                .reducer(new LongestTripReducerFactory())
                .submit(new LongestTripCollator());
    }

    public static void main(String[] args) {
        ClientQuery2 query2 = new ClientQuery2();

        Predicate<String[]> filter = line -> {
            int puId = Integer.parseInt(line[4]);
            int doId = Integer.parseInt(line[5]);
            return (puId != OUTSIDE_NYC_ID) &&
                    (doId != OUTSIDE_NYC_ID) &&
                    (query2.zonesMap.get(puId) != null) &&
                    (query2.zonesMap.get(doId) != null);
        };

        Function<String[], TripRowQ2> mapper = line -> new TripRowQ2(
                // Creamos el DTO optimizado para Q2
                query2.zonesMap.get(Integer.parseInt(line[4])).getZone(), // PULocation
                query2.zonesMap.get(Integer.parseInt(line[5])).getZone(), // DOLocation
                Double.parseDouble(line[6]), // trip_miles
                line[0],                    // company
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER) // request_datetime
        );

        query2.run(filter, mapper);
    }
}