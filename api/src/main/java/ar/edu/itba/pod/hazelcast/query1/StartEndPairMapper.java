package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.Pair;
import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashSet;
import java.util.Set;


public class StartEndPairMapper implements Mapper<Integer, TripRow, Pair<Integer, Integer>, Long>, HazelcastInstanceAware {
    private static final Long ONE = 1L;
    private transient Set<Integer> localZoneIds;

    @Override
    public void map(Integer integer, TripRow tripRow, Context<Pair<Integer, Integer>, Long> context) {
        int PULocationId = tripRow.getPULocationID();
        int DOLocationId = tripRow.getDOLocationID();

        if(localZoneIds.contains(PULocationId) && localZoneIds.contains(DOLocationId) && PULocationId != DOLocationId) {
            context.emit(new Pair<>(PULocationId, DOLocationId), ONE);
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance){
        IMap<Integer, ZonesRow> zonesMap = hazelcastInstance.getMap("zones");
        this.localZoneIds = new HashSet<>(zonesMap.keySet());
    }
}
