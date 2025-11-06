package ar.edu.itba.pod.hazelcast.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static final String SERVER_NAME = "g5-hazelcast";
    private static final String SERVER_PASS = "g5-hazelcast-pass";
    private static final String MANCENTER_URL = "http://localhost:8081/mancenter/";

    private static final String INTERFACE_LOOPBACK = "127.0.0.1";

    public static void main(String[] args) {
        logger.info(" Server Starting ...");

        // Config
        Config config = new Config();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName(SERVER_NAME).setPassword(SERVER_PASS);
        config.setGroupConfig(groupConfig);

        MulticastConfig multicastConfig = new MulticastConfig();

        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);

        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(Collections.singletonList(System.getProperty("interface", INTERFACE_LOOPBACK))).setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig().setInterfaces(interfacesConfig).setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);

        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setUrl(MANCENTER_URL)
                .setEnabled(false);
        config.setManagementCenterConfig(managementCenterConfig);

//        // Opcional: Logger detallado
//        java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
//        rootLogger.setLevel(Level.FINE);
//        for (Handler h : rootLogger.getHandlers()) {
//            h.setLevel(Level.FINE);
//        }

        // Start cluster
        Hazelcast.newHazelcastInstance(config);
    }
}