package ar.edu.itba.pod.hazelcast.client;




import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;


abstract class Client {
    protected Logger logger;
    protected BufferedWriter timesWriter;
    protected String address;
    protected String inPath;
    protected String outPath;
    protected HazelcastInstance hazelcastInstance;
    protected HashMap<Integer, ZonesRow> zonesMap = new HashMap<>();
    protected final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Client(String address, String inPath, String outPath){
        this.address = "localhost";
        this.inPath = inPath;
        this.outPath = "client/src/main/assembly/query3.csv";
        this.logger = LoggerFactory.getLogger(this.getClass());
        try {
            this.timesWriter = Files.newBufferedWriter(Path.of("client/src/main/assembly/times.csv"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean initializeHazelcast(){
        try{
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g05-hazelcast").setPassword("g05-hazelcast-pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress(this.address);

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
        }catch (Exception e){
            logger.error(e.getMessage());
            return false;
        }

        return true;
    }

    public void finalizeHazelcast(){
        hazelcastInstance.shutdown();
    }

    abstract int run();

    public void loadZonesData() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get("client/src/main/assembly/overlay/zones.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .map(line -> new ZonesRow(
                            Integer.parseInt(line[0]),
                            line[1],
                            line[2]
                    ))
                    .forEach(zone -> zonesMap.put(zone.getLocationID(), zone));
        }
    }

    public void printResults(List<String> toPrintList){
        try {
            Files.write(Path.of(outPath), toPrintList, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Error writing the file {}:", outPath, e);
        }
    }
}
