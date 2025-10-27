package ar.edu.itba.pod.hazelcast.client;



import ar.edu.itba.pod.hazelcast.query1.QueryOneResult;
import ar.edu.itba.pod.hazelcast.query1.StartEndPair;
import ar.edu.itba.pod.hazelcast.query3.*;
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

import ar.edu.itba.pod.hazelcast.common.*;

import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery3 {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("tpe2-g05 Client Starting ...");
        logger.info("grpc-com-patterns Client Starting ...");
        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g05-hazelcast").setPassword("g05-hazelcast-pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress("127.0.0.1");

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            // Read from csv file and loads maps
            IMap<Integer, ZonesRow> zonesMap = hazelcastInstance.getMap("zones");                       // key is the zones id

            IMap<Integer, TripRow> tripsMap = hazelcastInstance.getMap("trips");                        // key is PULocationId
            KeyValueSource<Integer, TripRow> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);

            final AtomicInteger tripsMapKey = new AtomicInteger();
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
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

            // ======================== Query ==========================
            JobTracker jobTracker = hazelcastInstance.getJobTracker("avgPriceByZoneAndComp");


            // Map Reduce Job
            Job<Integer, TripRow> job = jobTracker.newJob(tripsKeyValueSource);
            ICompletableFuture<List<Map.Entry<PickupCompanyPair, Double>>> future = job
                    .mapper(new PriceAvgMapper())
                    .reducer(new PickupCompanyPairReducerFactory())
                    .submit(new Query3Collator());

            // Process Data
            Map<PickupCompanyPair, Double> result = future.get();

            List<AvgPriceBoroughCompany> toReturn = new ArrayList<>();
            for(Map.Entry<PickupCompanyPair, Double> entry: result.entrySet()) {
                toReturn.add(new AvgPriceBoroughCompany(entry.getKey().getPULocation(), entry.getKey().getCompany(), entry.getValue()));
            }

            // Write Data
            String path = "client/src/main/assembly/query3.csv";
            Path outputPath = Paths.get(path);

            try (BufferedWriter fileWriter = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8);
                 PrintWriter printWriter = new PrintWriter(fileWriter)) {

                // Write header
                printWriter.println("pickUpBorough;company;avgFare");

                // Loop through results and write each line
                for (AvgPriceBoroughCompany resultLine : toReturn) {
                    printWriter.println(resultLine);
                }

            }
            // ======================== End Query =======================


        }finally {
            HazelcastClient.shutdownAll();
        }
    }
}
