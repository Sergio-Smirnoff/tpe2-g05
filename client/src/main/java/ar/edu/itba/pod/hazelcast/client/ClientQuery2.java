//package ar.edu.itba.pod.hazelcast.client;
//
//import ar.edu.itba.pod.hazelcast.query2.*;
//
//import com.hazelcast.core.ICompletableFuture;
//import com.hazelcast.core.IMap;
//import com.hazelcast.mapreduce.Job;
//import com.hazelcast.mapreduce.JobTracker;
//import com.hazelcast.mapreduce.KeyValueSource;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//public class ClientQuery2 extends Client<TripRowQ2,SortedSet<LongestTripResult>> {
//    private static final Integer QUERY_NUMBER = 2;
//
//    private static final int OUTSIDE_NYC_ID = 265; // ID de "Outside of NYC"
//
//    public ClientQuery2(final String address, final String inPath, final String outPath){
//        super(QUERY_NUMBER, address, inPath, outPath);
//    }
//
//    @Override
//    KeyValueSource<Integer, TripRowQ2> loadData() throws IOException {
//        this.loadZonesData();
//
//        // now loading the data
//        IMap<Integer, TripRowQ2> tripsMap = hazelcastInstance.getMap("trips-" + QUERY_NUMBER);// key is PULocationId
//        KeyValueSource<Integer, TripRowQ2> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
//        final AtomicInteger tripsMapKey = new AtomicInteger();
//        try (Stream<String> lines = Files.lines(Path.of(inPath).resolve(TRIPS_PATH), StandardCharsets.UTF_8)) {
//            lines.parallel().skip(1)
//                    .map(line -> line.split(";"))
//                    .filter(line -> {
//                        int puId = Integer.parseInt(line[4]);
//                        int doId = Integer.parseInt(line[5]);
//                        return (puId != OUTSIDE_NYC_ID) && (doId != OUTSIDE_NYC_ID);
//                    })
//                    .map(line -> new TripRowQ2(
//                        // Creamos el DTO optimizado para Q2
//                        zonesMap.get(Integer.parseInt(line[4])).getZone(), // PULocation
//                        zonesMap.get(Integer.parseInt(line[5])).getZone(), // DOLocation
//                        Double.parseDouble(line[6]), // trip_miles
//                        line[0],                    // company
//                        LocalDateTime.parse(line[1], DATE_TIME_FORMATTER) // request_datetime
//                    ))
//                    .forEach(trip -> {
//                        Integer uniqueId = tripsMapKey.getAndIncrement();
//                        tripsMap.put(uniqueId, trip);
//                    });
//        }
//
//        return tripsKeyValueSource;
//    }
//
//    @Override
//    void writeResults(SortedSet<LongestTripResult> results)  {
//        List<String> toPrint = new ArrayList<>();
//        // Header de Query 2
//        toPrint.add("pickUpZone;longestDOZone;longestPUDateTime;longestMiles;longestCompany");
//        toPrint.addAll(results.stream().map(Objects::toString).toList());
//        this.printResults(toPrint);
//    }
//
//    @Override
//    ICompletableFuture<SortedSet<LongestTripResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ2> keyValueSource) {
//        return jobTracker.newJob(keyValueSource)
//                .mapper(new LongestTripMapper())
//                .combiner(new LongestTripCombinerFactory())
//                .reducer(new LongestTripReducerFactory())
//                .submit(new LongestTripCollator());
//    }
//
//    public static void main(String[] args) {
//        ClientQuery2 query2 = new ClientQuery2(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
//        query2.run();
//    }
//}