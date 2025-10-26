package ar.edu.itba.pod.hazelcast.client;



import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.edu.itba.pod.hazelcast.common.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

public class Client {
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


            // ======================== Query 3 =============================
            // Tiene doble map and reduce porque primero tiene que filtrar los Borough que si van y luego hacer el avg de precio
            // Key Value Source
            MultiMap<Integer, TripRow> tripMultiMap = hazelcastInstance.getMultiMap("avgPriceByZoneAndComp");
            KeyValueSource<Integer, TripRow> tripKeyValueSource = KeyValueSource.fromMultiMap(tripMultiMap);

            // Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("avgPriceByZoneAndComp");

            // ======================== End Query 3 =============================

            // Text File Reading and Key Value Source Loading
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/trips-2025-01.csv"), StandardCharsets.UTF_8)) {
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
                        .forEach(trip -> tripMultiMap.put(trip.getPULocationID(), trip));
            }

            // Ejercicio 2.1
            // Check how many objects where loaded
            System.out.println(tripMultiMap.size());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }finally {
            HazelcastClient.shutdownAll();
        }
    }
}
