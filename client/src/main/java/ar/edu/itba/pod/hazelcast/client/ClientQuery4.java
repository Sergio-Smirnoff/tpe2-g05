package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query4.TripRowQ4;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.query4.DelayPerBoroughZoneCollator;
import ar.edu.itba.pod.hazelcast.query4.DelayPerBoroughZoneMapper;
import ar.edu.itba.pod.hazelcast.query4.DelayPerBoroughZoneReducerFactory;
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

public class ClientQuery4 extends Client{

    public ClientQuery4(final String address, final String inPath, final String outPath){
        super(address, inPath, outPath);
        super.initializeHazelcast();
    }

    @Override
    public int run(){
        logger.info("Query 4 Client Starting ...");
        try{
            timesWriter.write("# Times of Query 4 \n");
            // Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("query-4");

            // Args param for borough filtering
            String borough = "Manhattan";

            timesWriter.write("[INFO] Started Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            this.loadZonesData();

            // now loading the data
            IMap<Integer, TripRowQ4> tripsMap = hazelcastInstance.getMap("trips");// key is PULocationId
            KeyValueSource<Integer, TripRowQ4> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
            final AtomicInteger tripsMapKey = new AtomicInteger();
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
                lines.skip(1)
                        .map(line -> line.split(";"))
                        .filter(line ->{
                            ZonesRow PUZoneRow = zonesMap.get(Integer.parseInt(line[4]));
                            ZonesRow DOZoneRow = zonesMap.get(Integer.parseInt(line[5]));

                            return PUZoneRow != null && DOZoneRow != null && PUZoneRow.getBorough().compareTo(borough) == 0;
                        })
                        .map(line -> new TripRowQ4(
                                LocalDateTime.parse(line[1], dateTimeFormatter),
                                LocalDateTime.parse(line[2], dateTimeFormatter),
                                zonesMap.get(Integer.parseInt(line[4])).getZone(),
                                zonesMap.get(Integer.parseInt(line[5])).getZone()
                        ))
                        .forEach(trip -> tripsMap.put(tripsMapKey.getAndIncrement(), trip));
            }

            timesWriter.write("[INFO] Finished Loading Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            timesWriter.write("[INFO] Started Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            Job<Integer, TripRowQ4> job = jobTracker.newJob(tripsKeyValueSource);
            ICompletableFuture<SortedSet<QueryOneFourResult>> future = job
                    .mapper(new DelayPerBoroughZoneMapper())
                    .reducer(new DelayPerBoroughZoneReducerFactory())
                    .submit(new DelayPerBoroughZoneCollator());

            // Process Data
            SortedSet<QueryOneFourResult> results = future.get();

            timesWriter.write("[INFO] Finished Querying Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            timesWriter.write("[INFO] Started Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            List<String> toPrint = new ArrayList<>();
            // Add headers
            toPrint.add("pickUpZone;dropOffZone;delayInSeconds");
            toPrint.addAll(results.stream().map(Objects::toString).toList());
            this.printResults(toPrint);

            timesWriter.write("[INFO] Finished Writing Data: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            timesWriter.close();
        }catch ( Exception e ){
            logger.error("Error in the execution of the query 4: {}", e.getLocalizedMessage());
            return 1;
        }finally{
            finalizeHazelcast();
        }

        return 0;
    }


    // todo falta lo de borough, no se como se hace :) de ultima despues lo veo - martu
    public static void main(String[] args){

        ClientQuery4 query4 = new ClientQuery4(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));

        query4.run();

    }
}
