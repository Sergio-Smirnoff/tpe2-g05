package ar.edu.itba.pod.hazelcast.client;


import ar.edu.itba.pod.hazelcast.query3.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.hazelcast.common.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ClientQuery3 extends Client<TripRowQ3, AvgPriceBoroughCompany>{
    private static final Integer QUERY_NUMBER = 3;
    private static final int OUTSIDE_NYC_ID = 265;

    public ClientQuery3(final String address, final String inPath, final String outPath){
        super(QUERY_NUMBER, address, inPath, outPath);
    }

    @Override
    ICompletableFuture<List<AvgPriceBoroughCompany>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ3> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new PriceAvgMapper())
                .reducer(new PickupCompanyPairReducerFactory())
                .submit(new Query3Collator());
    }

    @Override
    String getCsvHeader() {
        return "pickUpBorough;company;avgFare";
    }

    public static void main(String[] args) {
        String serverAddress = "127.0.0.1"; // Connect to the server you just started.
        String inputPath = "client/src/main/assembly";          // Assumes a 'data' folder at the project root.
        String outputPath = "client/src/main/assembly";
        ClientQuery3 clientQuery3 = new ClientQuery3(serverAddress, inputPath, outputPath);

        Predicate<String[]> filter = line -> {
            int puId = Integer.parseInt(line[4]);

            return (puId != OUTSIDE_NYC_ID) &&
                    (clientQuery3.zonesMap.get(Integer.parseInt(line[4])) != null);
        };

        Function<String[], TripRowQ3> mapper = line -> new TripRowQ3(
                clientQuery3.zonesMap.get(Integer.parseInt(line[4])).getBorough(), // borough
                line[0],
                Double.parseDouble(line[7])
        );

        clientQuery3.run(filter, mapper);
    }
}
