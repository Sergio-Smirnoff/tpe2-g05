package ar.edu.itba.pod.hazelcast.client;


import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query1.*;
import com.hazelcast.core.ICompletableFuture;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class ClientQuery1 extends Client<TripRowQ1, QueryOneFourResult>{
    private static final Integer QUERY_NUMBER = 1;

    public ClientQuery1(){
        super(QUERY_NUMBER);
    }

    @Override
    @Deprecated
    ICompletableFuture<SortedSet<QueryOneFourResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ1> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new StartEndPairMapper())
                .combiner(new StartEndPairCombinerFactory())
                .reducer(new StartEndPairReducerFactory())
                .submit(new QueryOneCollator());
    }

    @Override
    String getCsvHeader() {
        return "pickUpZone;dropOffZone;trips";
    }

    public static void main(String[] args){
        ClientQuery1 query1 = new ClientQuery1();

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
