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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ClientQuery5 extends Client<TripRowQ5, TotalMilesResult> {
    private static final Integer QUERY_NUMBER = 5;

    public ClientQuery5(final String address, final String inPath, final String outPath){
        super(QUERY_NUMBER, address, inPath, outPath);
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
    String getCsvHeader() {
        return "company;year;month;milesYTD";
    }


    public static void main(String[] args) {
        String serverAddress = "127.0.0.1"; // Connect to the server you just started.
        String inputPath = "client/src/main/assembly";          // Assumes a 'data' folder at the project root.
        String outputPath = "client/src/main/assembly";
        ClientQuery5 query5 = new ClientQuery5(serverAddress, inputPath, outputPath);

        Predicate<String[]> filter = line -> true;

        Function<String[], TripRowQ5> mapper = line -> new TripRowQ5(
                line[0],
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                Double.parseDouble(line[6])
        );

        query5.run(filter, mapper);
    }
}
