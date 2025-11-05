package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ClientQuery1 extends Client<TripRowQ1, SortedSet<QueryOneFourResult>>{
    private static final Integer QUERY_NUMBER = 1;

    public ClientQuery1(final String address, final String inPath, final String outPath){
        super(QUERY_NUMBER, address, inPath, outPath);
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

        Predicate<String[]> filter = line -> {
            int puId = Integer.parseInt(line[4]);
            int doId = Integer.parseInt(line[5]);

            return  (puId != doId) &&
                    (query1.zonesMap.get(puId) != null) &&
                    (query1.zonesMap.get(doId) != null);

        };


        Function<String[], TripRowQ1> mapper = line -> new TripRowQ1(
                query1.zonesMap.get(Integer.parseInt(line[4])).getZone(),
                query1.zonesMap.get(Integer.parseInt(line[5])).getZone()
        );
        query1.run(filter, mapper);
    }
}
