package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query1.*;
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

public class ClientQuery1 extends Client<TripRowQ1, SortedSet<QueryOneFourResult>>{
    private static final Integer QUERY_NUMBER = 1;

    public ClientQuery1(final String address, final String inPath, final String outPath){
        super(QUERY_NUMBER, address, inPath, outPath);
    }

    @Override
    KeyValueSource<Integer, TripRowQ1> loadData() throws IOException {
        this.loadZonesData();

        // now loading the data
        IMap<Integer, TripRowQ1> tripsMap = hazelcastInstance.getMap("trips-" + QUERY_NUMBER);// key is PULocationId
        KeyValueSource<Integer, TripRowQ1> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
        final AtomicInteger tripsMapKey = new AtomicInteger();
        try (Stream<String> lines = Files.lines( Path.of(inPath).resolve(TRIPS_PATH), StandardCharsets.UTF_8)) {
            lines.parallel().skip(1)
                    .map(line -> line.split(";"))
                    .filter(line -> {
                        int puId = Integer.parseInt(line[4]);
                        int doId = Integer.parseInt(line[5]);

                        return (puId != doId) &&
                                (this.zonesMap.get(puId) != null) &&
                                (this.zonesMap.get(doId) != null);
                    })
                    .map(line -> new TripRowQ1(
                            zonesMap.get(Integer.parseInt(line[4])).getZone(),
                            zonesMap.get(Integer.parseInt(line[5])).getZone()
                    ))
                    .forEach(trip -> {
                        Integer uniqueId = tripsMapKey.getAndIncrement();
                        tripsMap.put(uniqueId, trip);
                    });
        }

        return tripsKeyValueSource;
    }

    @Override
    ICompletableFuture<SortedSet<QueryOneFourResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ1> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new StartEndPairMapper())
                .combiner(new StartEndPairCombinerFactory())
                .reducer(new StartEndPairReducerFactory())
                .submit(new QueryOneCollator());
    }

    @Override
    void writeResults(SortedSet<QueryOneFourResult> results) {
        List<String> toPrint = new ArrayList<>();
        // Add headers
        toPrint.add("pickUpZone;dropOffZone;trips");
        toPrint.addAll(results.stream().map(Objects::toString).toList());
        this.printResults(toPrint);
    }

    public static void main(String[] args){
        ClientQuery1 query1 = new ClientQuery1(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
        query1.run();
    }
}
