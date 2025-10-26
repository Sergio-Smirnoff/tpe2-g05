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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("tpe2-g05 Client Starting ...");
        logger.info("grpc-com-patterns Client Starting ...");
        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("l12345").setPassword("l12345-pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress("127.0.0.1");

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            // Key Value Source
            MultiMap<String, BikeRentalRow> rentalsMultiMap = hazelcastInstance.getMultiMap("rentalsByStartStation");
            KeyValueSource<String, BikeRentalRow> rentalsKeyValueSource = KeyValueSource.fromMultiMap(rentalsMultiMap);

            // Job Tracker
            JobTracker jobTracker = hazelcastInstance.getJobTracker("bikeRentals");

            // Text File Reading and Key Value Source Loading
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/trips-2025-01.csv"), StandardCharsets.UTF_8)) {
                lines.skip(1)
                        .map(line -> line.split(";"))
                        .map(line -> new BikeRentalRow(
                                line[0],
                                LocalDateTime.parse(line[1], dateTimeFormatter),
                                line[2],
                                LocalDateTime.parse(line[3], dateTimeFormatter),
                                Integer.parseInt(line[4]) == 1))
                        .forEach(bikeRental -> rentalsMultiMap.put(bikeRental.getStartStation(), bikeRental));
            }

            // Ejercicio 2.1
            // Check how many objects where loaded
            System.out.println(rentalsMultiMap.size());
        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}
