package ar.edu.itba.pod.hazelcast.client;

// Importamos las clases comunes y las específicas de la Query 2
import ar.edu.itba.pod.hazelcast.query2.*;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery2 extends Client {

    private static final int OUTSIDE_NYC_ID = 265; // ID de "Outside of NYC"

    public ClientQuery2(final String address, final String inPath, final String outPath) {
        super(address, inPath, outPath);
        super.initializeHazelcast();
    }

    @Override
    public int run() {
        logger.info("Query 2 Client Starting ...");
        try {
            timesWriter.write("# Times of Query 2 \n");
            JobTracker jobTracker = hazelcastInstance.getJobTracker("g05-query-2");

            timesWriter.write("[INFO] Started Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            this.loadZonesData(); // Carga el zonesMap de la clase base

            IMap<Integer, TripRowQ2> tripsMap = hazelcastInstance.getMap("g05-trips-q2");
            KeyValueSource<Integer, TripRowQ2> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
            final AtomicInteger tripsMapKey = new AtomicInteger();

            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/overlay/trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
                lines.parallel().skip(1)
                        .map(line -> line.split(";"))
                        .filter(line -> {
                            // Filtro de Query 2: Excluir viajes que inician o terminan en Outside of NYC
                            int puId = Integer.parseInt(line[4]);
                            int doId = Integer.parseInt(line[5]);
                            return (puId != OUTSIDE_NYC_ID) && (doId != OUTSIDE_NYC_ID);
                        })
                        .map(line -> new TripRowQ2( // Asumiendo que TripRowQ2 es un DTO DataSerializable
                                line[4], // PULocationID
                                line[5], // DOLocationID
                                Double.parseDouble(line[6]), // trip_miles
                                line[0], // company
                                LocalDateTime.parse(line[1], dateTimeFormatter) // request_datetime
                        ))
                        .forEach(trip -> {
                            Integer uniqueId = tripsMapKey.getAndIncrement();
                            tripsMap.put(uniqueId, trip);
                        });
            }

            timesWriter.write("[INFO] Finished Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            timesWriter.write("[INFO] Started Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            // Definición del Job
            Job<Integer, TripRowQ2> job = jobTracker.newJob(tripsKeyValueSource);
            // The code doesn't work due to two main reasons:
            // 1. The reducer() method expects a ReducerFactory<String, LongestTripValue, ValueOut>,
            //    but LongestTripReducerFactory probably does not implement the correct signature
            //    (should extend ReducerFactory<String, LongestTripValue, LongestTripValue> or similar).
            // 2. The constructor LongestTripCollator(HashMap<Integer,ZonesRow>) is undefined,
            //    there's likely no constructor in LongestTripCollator that accepts a zonesMap.
            //
            // To fix these, you must:
            //  - Ensure LongestTripReducerFactory has the right generics and implements/extends ReducerFactory<String,LongestTripValue,LongestTripValue>
            //  - Ensure LongestTripCollator has a constructor like `public LongestTripCollator(Map<Integer, ZonesRow> zonesMap)`
            //
            // Example (assuming both issues are fixed):
            ICompletableFuture<SortedSet<LongestTripResult>> future = job
                .mapper(new LongestTripMapper())
                .reducer(new LongestTripReducerFactory())
                .submit(new LongestTripCollator(this.zonesMap));

            // Procesar datos
            SortedSet<LongestTripResult> results = future.get(); // Query2Result debe implementar Comparable por pickUpZone

            timesWriter.write("[INFO] Finished Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            timesWriter.write("[INFO] Started Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            List<String> toPrint = new ArrayList<>();
            // Header de Query 2
            toPrint.add("pickUpZone;longestDOZone;longestPUDateTime;longestMiles;longestCompany");

            toPrint.addAll(results.stream().map(Objects::toString).toList());
            this.printResults(toPrint);
            timesWriter.write("[INFO] Finished Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            timesWriter.close();
        } catch (Exception e) {
            logger.error("Error in the execution of the query 2: {}", e.getLocalizedMessage());
            return 1;
        } finally {
            finalizeHazelcast();
        }

        return 0;
    }

    public static void main(String[] args) {
        ClientQuery2 query2 = new ClientQuery2(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
        query2.run();
    }
}