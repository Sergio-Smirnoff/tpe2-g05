package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;
import java.util.stream.Stream;


abstract class Client<T, K> {

    protected final Integer clientNumber;
    protected final String address;
    protected final String inPath;
    protected final String outPath;

    protected HazelcastInstance hazelcastInstance;
    // Mapa en memoria de zones para optimizar la carga de datos y reducir tráfico de red
    protected HashMap<Integer, ZonesRow> zonesMap = new HashMap<>();

    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private static final Logger timeLogger = Logger.getLogger("timeLogger");

    private static final String GROUP_NAME = "g05-hazelcast";
    private static final String GROUP_PASS = "g05-hazelcast-pass";

    protected static final String TRIPS_PATH = "trips.csv";
    protected static final String ZONES_PATH = "zones.csv";
    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Client(Integer clientNumber, String address, String inPath, String outPath){
        this.clientNumber = clientNumber;
        this.address = address;
        this.inPath = inPath;
        this.outPath = outPath;
        try {
            FileHandler fh = new FileHandler(Path.of(outPath).resolve("time" + clientNumber + ".csv").toString(), false);
            fh.setFormatter(new Formatter() {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss:SSSS")
                        .withZone(ZoneId.systemDefault());

                @Override
                public String format(LogRecord record) {
                    String timestamp = dtf.format(Instant.ofEpochMilli(record.getMillis()));
                    String thread = Thread.currentThread().getName();
                    String className = record.getSourceClassName();
                    return String.format("%s %s  [%s] %s (%s.java) - %s%n",
                            timestamp,
                            record.getLevel(),
                            thread,
                            className.substring(className.lastIndexOf(".") + 1),
                            className.substring(className.lastIndexOf(".") + 1),
                            record.getMessage());
                }
            });
            timeLogger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract KeyValueSource<Integer, T> loadData() throws IOException;
    abstract ICompletableFuture<K> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, T> keyValueSource);
    abstract void writeResults(K results) throws IOException;

    public void run(){
        logger.info("Query-" + clientNumber + ": Cliente iniciando ...");

        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName(GROUP_NAME).setPassword(GROUP_PASS);

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress(this.address);

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            timeLogger.info("Inicio de la lectura del archivo");

            KeyValueSource<Integer, T> valueSource = loadData();

            timeLogger.info("Fin de lectura del archivo");
            timeLogger.info("Inicio del trabajo map/reduce");

            JobTracker jobTracker = hazelcastInstance.getJobTracker("query-" + clientNumber);
            ICompletableFuture<K> future = executeMapReduce(jobTracker, valueSource);

            K results = future.get();

            writeResults(results);

            timeLogger.info("Fin del trabajo map/reduce");
        } catch ( Exception e ) {
            logger.log(Level.SEVERE, "Error en la ejecución", e);
        } finally {
            HazelcastClient.shutdownAll();
        }

    }

    protected void loadZonesData() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(Path.of(inPath).resolve(ZONES_PATH).toString()), StandardCharsets.UTF_8)) {
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

    protected void printResults(List<String> toPrintList){
        try {
            Files.write(Path.of(outPath).resolve("query" + clientNumber + ".csv"), toPrintList, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error escribiendo los resultados", e);
        }
    }
}
