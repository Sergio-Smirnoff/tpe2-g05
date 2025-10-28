package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;


public class StartEndPairMapper implements Mapper<Integer, TripRow, StartEndPair, Long>, HazelcastInstanceAware {
    private static final Long ONE = 1L;
    private transient IMap<Integer, ZonesRow> zonesMap;

    @Override
    public void map(Integer integer, TripRow tripRow, Context<StartEndPair, Long> context) {
        int PULocationId = tripRow.getPULocationID();
        int DOLocationId = tripRow.getDOLocationID();

        if(zonesMap.containsKey(PULocationId) && zonesMap.containsKey(DOLocationId) && PULocationId != DOLocationId) {
            context.emit(new StartEndPair(PULocationId, DOLocationId), ONE);
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance){
        this.zonesMap = hazelcastInstance.getMap("zones");
    }
}
