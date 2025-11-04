package ar.edu.itba.pod.hazelcast.query4.option2;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;

public class DelayPerBoroughZoneMapperOpt2 implements Mapper<Integer, TripRowQ42, String, Pair<String, Long>>, HazelcastInstanceAware {
    private transient IMap<Integer, ZonesRow> zonesMap;
    //private transient Map<Integer, ZonesRow> zonesMap;
    private final String desiredBorough;

    public DelayPerBoroughZoneMapperOpt2(String desiredBorough){
        this.desiredBorough = desiredBorough;
    }

    @Override
    public void map(Integer integer, TripRowQ42 tripRowQuery4, Context<String, Pair<String, Long>> context) {
        ZonesRow PUZoneRow = zonesMap.get(tripRowQuery4.getPULocationID());
        ZonesRow DOZoneRow = zonesMap.get(tripRowQuery4.getDOLocationID());

        if(PUZoneRow != null && DOZoneRow != null && PUZoneRow.getBorough().compareTo(desiredBorough) == 0) {
            String PUZone = PUZoneRow.getZone();
            String DOZone = DOZoneRow.getZone();

            context.emit(PUZone, new Pair<>(DOZone, tripRowQuery4.getDelayInSeconds()));
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        //this.zonesMap = new HashMap<>(hazelcastInstance.getMap("zones"));
        this.zonesMap = hazelcastInstance.getMap("zones");
    }
}
