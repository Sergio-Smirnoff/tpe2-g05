package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.query1.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClientQuery1 extends Client{

    public ClientQuery1(final String address, final String inPath, final String outPath){
        super(address, inPath, outPath);
        super.initializeHazelcast();
    }

    @Override
    public int run(){
        logger.info("Query 1 Client Starting ...");
        try{
            timesWriter.write("# Times of Query 1 \n");
            // Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("query-1");

            timesWriter.write("[INFO] Started Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            this.loadZonesData();

            // now loading the data
            IMap<Integer, TripRowQ1> tripsMap = hazelcastInstance.getMap("trips");// key is PULocationId
            KeyValueSource<Integer, TripRowQ1> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
            final AtomicInteger tripsMapKey = new AtomicInteger();
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/overlay/trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
                lines.parallel().skip(1)
                        .map(line -> line.split(";"))
                        .filter(line -> {
                            int puId = Integer.parseInt(line[4]);
                            int doId = Integer.parseInt(line[5]);
                            if ( puId == doId )
                                return false;
                            return (this.zonesMap.get(puId) != null) && (this.zonesMap.get(doId) != null);
                        })
                        .map(line -> new TripRowQ1(
                                zonesMap.get(Integer.parseInt(line[4])).getZone(),
                                zonesMap.get(Integer.parseInt(line[5])).getZone(),
                                Double.parseDouble(line[6])
                        ))
                        .forEach(trip -> {
                            Integer uniqueId = tripsMapKey.getAndIncrement();
                            tripsMap.put(uniqueId, trip);
                        });
            }

            timesWriter.write("[INFO] Finished Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            timesWriter.write("[INFO] Started Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            Job<Integer, TripRowQ1> job = jobTracker.newJob(tripsKeyValueSource);
            ICompletableFuture<SortedSet<QueryOneFourResult>> future = job
                    .mapper(new StartEndPairMapper())
                    .reducer(new StartEndPairReducerFactory())
                    .submit(new QueryOneCollator());

            // Process Data
            SortedSet<QueryOneFourResult> results = future.get();

            timesWriter.write("[INFO] Finished Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            timesWriter.write("[INFO] Started Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            List<String> toPrint = results.stream().map(Objects::toString).toList();
            this.printResults(toPrint);

            timesWriter.write("[INFO] Finished Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            timesWriter.close();
        }catch ( Exception e ){
            logger.error("Error in the execution of the query 1: {}", e.getLocalizedMessage());
            return 1;
        }finally{
            finalizeHazelcast();
        }

        return 0;
    }


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        ClientQuery1 query1 = new ClientQuery1(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));

        query1.run();

    }
}
