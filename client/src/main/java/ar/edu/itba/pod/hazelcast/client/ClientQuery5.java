package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.query5.*;
import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesResult;
import ar.edu.itba.pod.hazelcast.query5.objects.TripRowQ5;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class ClientQuery5 extends Client<TripRowQ5, TotalMilesResult> {
    private static final Integer QUERY_NUMBER = 5;

    public ClientQuery5(){
        super(QUERY_NUMBER);
    }

    @Override
    @Deprecated
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
        ClientQuery5 query5 = new ClientQuery5();

        Predicate<String[]> filter = line -> true;

        Function<String[], TripRowQ5> mapper = line -> new TripRowQ5(
                line[0],
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                Double.parseDouble(line[6])
        );

        query5.run(filter, mapper);
    }
}
