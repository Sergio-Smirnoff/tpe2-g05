package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.query5.*;
import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesResult;
import ar.edu.itba.pod.hazelcast.query5.objects.TripRowQ5;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientQuery5 extends Client<TripRowQ5, List<TotalMilesResult>> {
    private static final Integer QUERY_NUMBER = 5;

    public ClientQuery5(final String address, final String inPath, final String outPath){
        super(QUERY_NUMBER, address, inPath, outPath);
    }

    @Override
    KeyValueSource<Integer, TripRowQ5> loadData() throws IOException {
        IMap<Integer, TripRowQ5> tripsMap = hazelcastInstance.getMap("trips-" + QUERY_NUMBER);
        KeyValueSource<Integer, TripRowQ5> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);

        final AtomicInteger tripsMapKey = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Path.of(inPath).resolve(TRIPS_PATH), StandardCharsets.UTF_8)) {
            lines.skip(1).parallel()
                    .map(line -> line.split(";"))
                    .map(line -> new TripRowQ5(
                            line[0],
                            LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                            Double.parseDouble(line[6])
                    ))
                    .forEach(trip -> {
                        Integer uniqueId = tripsMapKey.getAndIncrement();
                        tripsMap.put(uniqueId, trip);
                    });
        }

        return tripsKeyValueSource;
    }

    @Override
    ICompletableFuture<List<TotalMilesResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ5> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new TotalMilesMapper())
                .combiner(new TotalMilesCombinerFactory())
                .reducer(new TotalMilesReducerFactory())
                .submit(new TotalMilesCollator());
    }

    @Override
    void writeResults(List<TotalMilesResult> results) {
        List<String> toPrint = new ArrayList<>();
        toPrint.add("company;year;month;milesYTD");
        toPrint.addAll(results.stream().map(Objects::toString).toList());
        this.printResults(toPrint);
    }

    public static void main(String[] args) {
        ClientQuery5 query = new ClientQuery5(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
        query.run();
    }
}
