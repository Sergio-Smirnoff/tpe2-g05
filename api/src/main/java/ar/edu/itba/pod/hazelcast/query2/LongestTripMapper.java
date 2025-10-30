package ar.edu.itba.pod.hazelcast.query2;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.TripRowFull;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class LongestTripMapper implements Mapper<Integer, TripRowQ2, String, LongestTripValue>{

    private static final String OUTSIDE_OF_NYC_ID = "265";

    public LongestTripMapper() {}

    @Override
    public void map(Integer integer, TripRowQ2 tripRow, Context<String, LongestTripValue> context) {
        String PULocationID = tripRow.getPULocation();
        String DOLocationID = tripRow.getDOLocation();

        if (!Objects.equals(PULocationID, OUTSIDE_OF_NYC_ID) && !Objects.equals(DOLocationID, OUTSIDE_OF_NYC_ID)) {
            context.emit(PULocationID, new LongestTripValue(tripRow.getTrip_miles(),
                    tripRow.getRequestTime(),
                    DOLocationID,
                    tripRow.getCompany()));
        }

    }


}
