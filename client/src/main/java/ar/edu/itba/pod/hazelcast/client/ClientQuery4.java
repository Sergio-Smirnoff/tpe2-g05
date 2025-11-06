package ar.edu.itba.pod.hazelcast.client;

import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.query4.*;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class ClientQuery4 extends Client<TripRowQ4, QueryOneFourResult>{
    private static final Integer QUERY_NUMBER = 4;
    private final String borough;

    public ClientQuery4(final String borough){
        super(QUERY_NUMBER);
        this.borough = borough;
    }

    @Override
    @Deprecated
    ICompletableFuture<SortedSet<QueryOneFourResult>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ4> keyValueSource) {
        return jobTracker.newJob(keyValueSource)
                .mapper(new DelayPerBoroughZoneMapper())
                .combiner(new DelayPerBoroughZoneCombinerFactory())
                .reducer(new DelayPerBoroughZoneReducerFactory())
                .submit(new DelayPerBoroughZoneCollator());
    }

    @Override
    String getCsvHeader() {
        return "pickUpZone;dropOffZone;delayInSeconds";
    }


    public static void main(String[] args){
        String borough = System.getProperty("borough", "Manhattan");
        ClientQuery4 query4 = new ClientQuery4(borough);

        Predicate<String[]> filter = line ->{
            ZonesRow PUZoneRow = query4.zonesMap.get(Integer.parseInt(line[4]));
            ZonesRow DOZoneRow = query4.zonesMap.get(Integer.parseInt(line[5]));

            return PUZoneRow != null
                    && DOZoneRow != null
                    && PUZoneRow.getBorough().equals(query4.borough);
        };

        Function<String[], TripRowQ4> mapper = line -> new TripRowQ4(
                LocalDateTime.parse(line[1], DATE_TIME_FORMATTER),
                LocalDateTime.parse(line[2], DATE_TIME_FORMATTER),
                query4.zonesMap.get(Integer.parseInt(line[4])).getZone(),
                query4.zonesMap.get(Integer.parseInt(line[5])).getZone()
        );

        query4.run(filter, mapper);
    }
}
