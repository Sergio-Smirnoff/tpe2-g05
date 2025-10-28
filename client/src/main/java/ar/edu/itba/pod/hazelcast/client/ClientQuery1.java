package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.query1.QueryOneResult;
import ar.edu.itba.pod.hazelcast.query1.StartEndPair;
import ar.edu.itba.pod.hazelcast.query1.StartEndPairMapper;
import ar.edu.itba.pod.hazelcast.query1.StartEndPairReducerFactory;
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
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery1 {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        logger.info("Query 1 Client Starting ...");

        try {
            BufferedWriter timesLogger = Files.newBufferedWriter(Path.of("client/src/main/assembly/times-query-one.txt"), StandardCharsets.UTF_8);
            timesLogger.write("### Times of Query 1 ###\n");

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
            JobTracker jobTracker = hazelcastInstance.getJobTracker("query-1");

            // Read from csv file and loads maps
            IMap<Integer, ZonesRow> zonesMap = hazelcastInstance.getMap("zones");                       // key is the zones id

            IMap<Integer, TripRow> tripsMap = hazelcastInstance.getMap("trips");                        // key is PULocationId
            KeyValueSource<Integer, TripRow> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);

            final AtomicInteger tripsMapKey = new AtomicInteger();
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            timesLogger.write("[INFO] Start Data Loading: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/trips-2025-01-mini-400000.csv"), StandardCharsets.UTF_8)) {
                lines.skip(1)
                        .map(line -> line.split(";"))
                        .map(line -> new TripRow(
                                line[0],
                                LocalDateTime.parse(line[1], dateTimeFormatter),
                                LocalDateTime.parse(line[2], dateTimeFormatter),
                                LocalDateTime.parse(line[3], dateTimeFormatter),
                                Integer.parseInt(line[4]),
                                Integer.parseInt(line[5]),
                                Double.parseDouble(line[6]),
                                Double.parseDouble(line[7])
                        ))
                        .forEach(trip -> tripsMap.put(tripsMapKey.getAndIncrement(), trip));
            }

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

            timesLogger.write("[INFO] End Data Loading: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            // Map Reduce Job
            timesLogger.write("[INFO] Start Map Reduce Job: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");
            Job<Integer, TripRow> job = jobTracker.newJob(tripsKeyValueSource);
            ICompletableFuture<Map<StartEndPair, Long>> future = job
                    .mapper(new StartEndPairMapper())
                    .reducer(new StartEndPairReducerFactory())
                    .submit();

            // Process Data
            Map<StartEndPair, Long> result = future.get();
            timesLogger.write("[INFO] End Map Reduce Job: " + LocalDateTime.now().format(dateTimeFormatter) + "\n");

            Map<Integer, ZonesRow> localZonesMap = new HashMap<>(zonesMap);
            SortedSet<QueryOneResult> finalResults = new TreeSet<>(QueryOneResult.getComparator());
            for(Map.Entry<StartEndPair, Long> entry: result.entrySet()) {
                String startZone = localZonesMap.get(entry.getKey().getStartZone()).getZone();
                String endZone = localZonesMap.get(entry.getKey().getEndZone()).getZone();
                Long count = entry.getValue();
                finalResults.add(new QueryOneResult(startZone, endZone, count));
            }

            // Write Data
            String path = "client/src/main/assembly/query1.csv";
            Path outputPath = Paths.get(path);

            try (BufferedWriter fileWriter = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8);
                 PrintWriter printWriter = new PrintWriter(fileWriter)) {

                // Write header
                printWriter.println("pickUpZone;dropOffZone;trips");

                // Loop through results and write each line
                for (QueryOneResult resultLine : finalResults) {
                    printWriter.println(resultLine.toCsvLine());
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
