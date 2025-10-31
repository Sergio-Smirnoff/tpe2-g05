package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query4.*;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery4 extends Client<TripRowQ4, SortedSet<QueryOneFourResult>>{
    private static final Integer QUERY_NUMBER = 4;
    public ClientQuery4(final String address, final String inPath, final String outPath){
        super(QUERY_NUMBER, address, inPath, outPath);
    }

    @Override
    KeyValueSource<Integer, TripRowQ4> loadData() throws IOException {
        // Args param for borough filtering
        String borough = "Manhattan";

        this.loadZonesData();

        // now loading the data
        IMap<Integer, TripRowQ4> tripsMap = hazelcastInstance.getMap("trips4");// key is PULocationId
        KeyValueSource<Integer, TripRowQ4> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
        final AtomicInteger tripsMapKey = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Path.of(inPath).resolve("trips-2025-01-mini.csv"), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .filter(line ->{
                        ZonesRow PUZoneRow = zonesMap.get(Integer.parseInt(line[4]));
                        ZonesRow DOZoneRow = zonesMap.get(Integer.parseInt(line[5]));

                        return PUZoneRow != null && DOZoneRow != null && PUZoneRow.getBorough().compareTo(borough) == 0;
                    })
                    .map(line -> new TripRowQ4(
                            LocalDateTime.parse(line[1], dateTimeFormatter),
                            LocalDateTime.parse(line[2], dateTimeFormatter),
                            zonesMap.get(Integer.parseInt(line[4])).getZone(),
                            zonesMap.get(Integer.parseInt(line[5])).getZone()
                    ))
                    .forEach(trip -> tripsMap.put(tripsMapKey.getAndIncrement(), trip));
        }

        return tripsKeyValueSource;
    }

    @Override
    ICompletableFuture<SortedSet<QueryOneFourResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ4> keyValueSource) {
        Job<Integer, TripRowQ4> job = jobTracker.newJob(keyValueSource);
        ICompletableFuture<SortedSet<QueryOneFourResult>> future = job
                .mapper(new DelayPerBoroughZoneMapper())
                .combiner(new DelayPerBoroughZoneCombinerFactory())
                .reducer(new DelayPerBoroughZoneReducerFactory())
                .submit(new DelayPerBoroughZoneCollator());

        return future;
    }

    @Override
    void writeResults(SortedSet<QueryOneFourResult> results) throws IOException {
        List<String> toPrint = new ArrayList<>();
        // Add headers
        toPrint.add("pickUpZone;dropOffZone;delayInSeconds");
        toPrint.addAll(results.stream().map(Objects::toString).toList());
        this.printResults(toPrint);
    }


    // todo falta lo de borough, no se como se hace :) de ultima despues lo veo - martu
    public static void main(String[] args){
        ClientQuery4 query4 = new ClientQuery4(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
        query4.run();
    }
}
