package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.query1.TripRowQ1;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
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

    abstract ICompletableFuture<K> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, T> keyValueSource);
    abstract void writeResults(K results) throws IOException;

    public void run(Predicate<? super String[]> filter, Function<String[], T> mapper){
        logger.info("Query-" + clientNumber + ": Cliente iniciando ...");

        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName(GROUP_NAME).setPassword(GROUP_PASS);

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.addAddress(this.address);

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);
            clientConfig.setProperty("hazelcast.client.heartbeat.timeout", "300000");

            // Node Client
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

            timeLogger.info("Inicio de la lectura del archivo");

            KeyValueSource<Integer, T> valueSource = loadTripsData(filter, mapper); ;

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

    protected KeyValueSource<Integer, T> loadTripsData(Predicate<? super String[]> filter, Function<String[], T> mapper) throws IOException {
        loadZonesData();

        IMap<Integer, T> tripsMap = hazelcastInstance.getMap("trips-" + clientNumber);
        KeyValueSource<Integer, T> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);

        final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
        final int BATCH_SIZE = 10_000;
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);
        final AtomicInteger tripsMapKey = new AtomicInteger();

        try (Stream<String> lines = Files.lines( Path.of(inPath).resolve(TRIPS_PATH), StandardCharsets.UTF_8)) {
            Iterator<String> iterator = lines.iterator();
            List<String> batch = new ArrayList<>(BATCH_SIZE);

            // skip header
            if(iterator.hasNext()) iterator.next();

            // process data
            while(iterator.hasNext()){
                batch.add(iterator.next());

                if(batch.size() == BATCH_SIZE) {
                    executorService.submit(new TripBatchLoader<>(
                            new ArrayList<>(batch),
                            tripsMap,
                            tripsMapKey,
                            filter,
                            mapper
                    ));

                    batch.clear();
                }
            }

            if(!batch.isEmpty()){
                executorService.submit(new TripBatchLoader<>(
                        new ArrayList<>(batch),
                        tripsMap,
                        tripsMapKey,
                        filter,
                        mapper
                ));
            }
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.MINUTES);
        } catch (InterruptedException e){
            logger.log(Level.SEVERE, "Error en la ejecución", e);
            Thread.currentThread().interrupt();
        }

        return tripsKeyValueSource;
    }

    private record TripBatchLoader<T>(
            List<String> lines,
            IMap<Integer, T> tripsMap,
            AtomicInteger tripsMapKey,
            Predicate<? super String[]> filter,
            Function<String[], T> mapper
    ) implements Runnable{

        @Override
        public void run() {
            Map<Integer, T> localTripsBatch = new HashMap<>(lines.size());
            for (String line: lines){
                String[] parts = line.split(";");

                if(filter().test(parts)){
                    T trip = mapper.apply(parts);
                    if(trip != null){
                        localTripsBatch.put(tripsMapKey.getAndIncrement(), trip);
                    }
                }
            }
            tripsMap.putAll(localTripsBatch);
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
