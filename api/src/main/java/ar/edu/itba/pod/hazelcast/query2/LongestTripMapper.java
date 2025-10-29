package ar.edu.itba.pod.hazelcast.query2;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashSet;
import java.util.Set;

public class LongestTripMapper implements Mapper<Integer, TripRow, Integer, LongestTripValue>, HazelcastInstanceAware {

    private static final int OUTSIDE_OF_NYC_ID = 265;
    private transient Set<Integer> localZoneIds;

    public LongestTripMapper() {}

    @Override
    public void map(Integer integer, TripRow tripRow, Context<Integer, LongestTripValue> context) {
        Integer PULocationID = tripRow.getPULocationID();
        Integer DOLocationID = tripRow.getDOLocationID();

        if (localZoneIds.contains(PULocationID) && PULocationID != OUTSIDE_OF_NYC_ID && DOLocationID != OUTSIDE_OF_NYC_ID) {
            context.emit(PULocationID, new LongestTripValue(tripRow.getTrip_miles(),
                    tripRow.getRequestTime(),
                    DOLocationID,
                    tripRow.getCompany()));
        }

    }
    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        IMap<Integer, ZonesRow> zonesMap = hazelcastInstance.getMap("zones");
        this.localZoneIds = new HashSet<>(zonesMap.keySet());
    }


}
