package ar.edu.itba.pod.hazelcast.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static final String SERVER_NAME = "g05-hazelcast";
    private static final String SERVER_PASS = "g05-hazelcast-pass";
    private static final String MANCENTER_URL = "http://localhost:8081/mancenter/";

    private static final String INTERFACE_LOOPBACK = "127.0.0.1";
    private static final String INTERFACE_NETWORK_A = "10.6.0.*";
    private static final String INTERFACE_NETWORK_C = "192.168.1.*";

    private static final List<String> ALLOWED_INTERFACES = Arrays.asList(
            INTERFACE_LOOPBACK,
            INTERFACE_NETWORK_A,
            INTERFACE_NETWORK_C
    );

    public static void main(String[] args) {
        logger.info(" Server Starting ...");

        // Config
        Config config = new Config();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName(SERVER_NAME).setPassword(SERVER_PASS);
        config.setGroupConfig(groupConfig);

        // Network Config
        NetworkConfig networkConfig = config.getNetworkConfig();

        networkConfig.getJoin().getTcpIpConfig().setEnabled(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(true);

        networkConfig.getInterfaces()
                .setEnabled(true)
                .setInterfaces(ALLOWED_INTERFACES);

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