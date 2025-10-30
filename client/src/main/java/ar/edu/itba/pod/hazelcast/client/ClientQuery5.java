package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.query5.*;
import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesKey;
import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesResult;
import ar.edu.itba.pod.hazelcast.query5.objects.TripRowQ5;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClientQuery5 extends Client {

    public ClientQuery5(final String address, final String inPath, final String outPath){
        super(address, inPath, outPath);
        super.initializeHazelcast();
    }

    @Override
    public int run(){
        logger.info("Query 5 Client Starting ...");
        try {
            timesWriter.write("# Times of Query 5 \n");
            // Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("query-5");

            logger.info("Started Loading Data");
            timesWriter.write("[INFO] Started Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            IMap<Integer, TripRowQ5> tripsMap = hazelcastInstance.getMap("trips");
            KeyValueSource<Integer, TripRowQ5> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
            final AtomicInteger tripsMapKey = new AtomicInteger();
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/overlay/trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
                lines.skip(1).parallel()
                        .map(line -> line.split(";"))
                        .map(line -> new TripRowQ5(
                                line[0],
                                LocalDateTime.parse(line[1], dateTimeFormatter),
                                Double.parseDouble(line[6])
                        ))
                        .forEach(trip -> {
                            Integer uniqueId = tripsMapKey.getAndIncrement();
                            tripsMap.put(uniqueId, trip);
                        });
            }

            timesWriter.write("[INFO] Finished Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            logger.info("Started Querying Data");
            timesWriter.write("[INFO] Started Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            Job<Integer, TripRowQ5> job = jobTracker.newJob(tripsKeyValueSource);
            ICompletableFuture<List<TotalMilesResult>> future = job
                    .mapper(new TotalMilesMapper())
                    .combiner(new TotalMilesCombinerFactory())
                    .reducer(new TotalMilesReducerFactory())
                    .submit(new TotalMilesCollator());

            List<TotalMilesResult> results = future.get();

            timesWriter.write("[INFO] Finished Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            logger.info("Started Writing Data");
            timesWriter.write("[INFO] Started Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            LinkedList<String> toPrint = results.stream()
                    .map(Objects::toString)
                    .collect(Collectors.toCollection(LinkedList::new));
            toPrint.addFirst("company;year;month;milesYTD");
            this.printResults(toPrint);

            timesWriter.write("[INFO] Finished Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            timesWriter.close();
        } catch ( Exception e ){
            logger.error("Error in the execution of the query 5: {}", e.getLocalizedMessage());
            return 1;
        } finally {
            finalizeHazelcast();
        }

        return 0;
    }


    public static void main(String[] args) {
        ClientQuery5 query = new ClientQuery5(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
        query.run();
    }
}
