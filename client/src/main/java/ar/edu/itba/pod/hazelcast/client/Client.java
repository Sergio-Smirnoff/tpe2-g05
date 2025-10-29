package ar.edu.itba.pod.hazelcast.client;




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


abstract class Client {
    protected Logger logger;
    protected BufferedWriter timesWriter;
    protected String address;
    protected String inPath;
    protected String outPath;
    protected HazelcastInstance hazelcastInstance;

    public Client(String address, String inPath, String outPath){
        this.address = address;
        this.inPath = inPath;
        this.outPath = outPath;
        this.logger = LoggerFactory.getLogger(this.getClass());
        try {
            this.timesWriter = Files.newBufferedWriter(Path.of(outPath), StandardCharsets.UTF_8);
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
        }finally {
            HazelcastClient.shutdownAll();
        }
        return true;
    }

    public int run() {
        initializeHazelcast();
        return 0;
    }
}
