package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query4.*;
import ar.edu.itba.pod.hazelcast.query4.option2.DelayPerBoroughZoneMapperOpt2;
import ar.edu.itba.pod.hazelcast.query4.option2.TripRowQ42;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery42 extends Client<TripRowQ42, SortedSet<QueryOneFourResult>> {
    private static final Integer QUERY_NUMBER = 42;
    private final String borough;

    public ClientQuery42(final String address, final String inPath, final String outPath, final String borough){
        super(QUERY_NUMBER, address, inPath, outPath);
        this.borough = borough;
    }

    @Override
    KeyValueSource<Integer, TripRowQ42> loadData() throws IOException {
        IMap<Integer, ZonesRow> zonesMap = hazelcastInstance.getMap("zones");
        try (Stream<String> lines = Files.lines(Path.of(inPath).resolve(ZONES_PATH), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .map(line -> new ZonesRow(
                            Integer.parseInt(line[0]),
                            line[1],
                            line[2]
                    ))
                    .forEach(zone -> zonesMap.put(zone.getLocationID(), zone));
        }
        // now loading the data
        IMap<Integer, TripRowQ42> tripsMap = hazelcastInstance.getMap("trips-" + QUERY_NUMBER);// key is PULocationId
        KeyValueSource<Integer, TripRowQ42> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
        final AtomicInteger tripsMapKey = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Path.of(inPath).resolve(TRIPS_PATH), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .map(line -> new TripRowQ42(
                            LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                            LocalDateTime.parse(line[2], DATE_TIME_FORMATTER),
                            Integer.parseInt(line[4]),
                            Integer.parseInt(line[5])
                    ))
                    .forEach(trip -> tripsMap.put(tripsMapKey.getAndIncrement(), trip));
        }

        return tripsKeyValueSource;
    }

    @Override
    ICompletableFuture<SortedSet<QueryOneFourResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ42> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new DelayPerBoroughZoneMapperOpt2(borough))
                .reducer(new DelayPerBoroughZoneReducerFactory())
                .submit(new DelayPerBoroughZoneCollator());
    }

    @Override
    void writeResults(SortedSet<QueryOneFourResult> results) {
        List<String> toPrint = new ArrayList<>();
        // Add headers
        toPrint.add("pickUpZone;dropOffZone;delayInSeconds");
        toPrint.addAll(results.stream().map(Objects::toString).toList());
        this.printResults(toPrint);
    }


    public static void main(String[] args){
        String serverAddress = "127.0.0.1"; // Connect to the server you just started.
        String inputPath = "client/src/main/assembly";          // Assumes a 'data' folder at the project root.
        String outputPath = "client/src/main/assembly";
        String borough = "Manhattan";
        ClientQuery42 query42 = new ClientQuery42(serverAddress, inputPath, outputPath, borough);
        query42.run();
    }
}
