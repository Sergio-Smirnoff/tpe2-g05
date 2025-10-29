package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.common.TripRowQuery4;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.query4.DelayPerBoroughZoneCollator;
import ar.edu.itba.pod.hazelcast.query4.DelayPerBoroughZoneMapper;
import ar.edu.itba.pod.hazelcast.query4.DelayPerBoroughZoneReducerFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery4 {
    private static final Logger logger = LoggerFactory.getLogger(ClientQuery4.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        logger.info("Query 4_2 Client Starting ...");

        try {
            BufferedWriter timesLogger = Files.newBufferedWriter(Path.of("client/src/main/assembly/times-query-four-2.txt"), StandardCharsets.UTF_8);
            timesLogger.write("### Times of Query 4_2 ###\n");

            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g05-hazelcast").setPassword("g05-hazelcast-pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress("127.0.0.1");

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            // Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("query-4-2");

            // Read from csv file and loads maps

            /*
             *      in this case, we preload de zones into a local hashmap and then use it to load
             *      the trips map with only the zones that are in the zonesmap
             */
            Map<Integer, ZonesRow> zonesMap = new HashMap<>();                                                  // key is the zones id

            IMap<Integer, TripRowQuery4> tripsMap = hazelcastInstance.getMap("trips");                        // key is PULocationId
            KeyValueSource<Integer, TripRowQuery4> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            String borough = "Manhattan";
            final AtomicInteger tripsMapKey = new AtomicInteger();

            timesLogger.write("[INFO] Start Data Loading: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            /*
             *  Para las zonas necesito los tres campos
             */
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/zones.csv"), StandardCharsets.UTF_8)) {
                lines.skip(1)
                        .map(line -> line.split(";"))
                        .map(line -> new ZonesRow(
                                Integer.parseInt(line[0]),
                                line[1],
                                line[2]
                        ))
                        .forEach(zone -> zonesMap.put(zone.getLocationID(), zone));
            }

            /*
            *    Para los trips uso request time, pickup time (ambos LocalDateTime, pu zone como string y do zone como string
            */
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
                lines.skip(1)
                        .map(line -> line.split(";"))
                        .filter(line ->{
                            ZonesRow PUZoneRow = zonesMap.get(Integer.parseInt(line[4]));
                            ZonesRow DOZoneRow = zonesMap.get(Integer.parseInt(line[5]));

                            return PUZoneRow != null && DOZoneRow != null && PUZoneRow.getBorough().compareTo(borough) == 0;
                        })
                        .map(line -> new TripRowQuery4(
                                LocalDateTime.parse(line[1], dateTimeFormatter),
                                LocalDateTime.parse(line[2], dateTimeFormatter),
                                zonesMap.get(Integer.parseInt(line[4])).getZone(),
                                zonesMap.get(Integer.parseInt(line[5])).getZone()
                        ))
                        .forEach(trip -> tripsMap.put(tripsMapKey.getAndIncrement(), trip));
            }

            timesLogger.write("[INFO] End Data Loading: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            // Map Reduce Job
            timesLogger.write("[INFO] Start Map Reduce Job: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            Job<Integer, TripRowQuery4> job = jobTracker.newJob(tripsKeyValueSource);
            ICompletableFuture<SortedSet<QueryOneFourResult>> future = job
                    .mapper(new DelayPerBoroughZoneMapper())
                    .reducer(new DelayPerBoroughZoneReducerFactory())
                    .submit(new DelayPerBoroughZoneCollator());

            // Process Data
            SortedSet<QueryOneFourResult> results = future.get();
            timesLogger.write("[INFO] End Map Reduce Job: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            // Write Data
            String path = "client/src/main/assembly/query4_2.csv";
            Path outputPath = Paths.get(path);

            try (BufferedWriter fileWriter = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8);
                 PrintWriter printWriter = new PrintWriter(fileWriter)) {

                // Write header
                printWriter.println("pickUpZone;dropOffZone;delayInSeconds");

                // Loop through results and write each line
                for (QueryOneFourResult result: results) {
                    printWriter.println(result.toString());
                }

            }
            timesLogger.close();

            // Check items loaded
            //System.out.printf("trip map size (small) : %d", tripsMap.size());
            //System.out.printf("zone map size : %d", zonesMap.size());
        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}
