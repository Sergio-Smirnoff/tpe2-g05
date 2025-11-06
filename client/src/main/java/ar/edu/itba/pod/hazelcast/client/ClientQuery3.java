package ar.edu.itba.pod.hazelcast.client;


import ar.edu.itba.pod.hazelcast.query3.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class ClientQuery3 extends Client<TripRowQ3, AvgPriceBoroughCompany>{
    private static final Integer QUERY_NUMBER = 3;
    private static final int OUTSIDE_NYC_ID = 265;

    public ClientQuery3(){
        super(QUERY_NUMBER);
    }

    @Override
    @Deprecated
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
        ClientQuery3 clientQuery3 = new ClientQuery3();

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
