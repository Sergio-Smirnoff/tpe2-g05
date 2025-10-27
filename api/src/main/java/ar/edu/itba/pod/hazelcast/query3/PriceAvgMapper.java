package ar.edu.itba.pod.hazelcast.query3;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PriceAvgMapper implements Mapper<Integer, TripRow, PickupCompanyPair, Double>, HazelcastInstanceAware {
    private transient HashMap<Integer, ZonesRow> localZoneIds;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        IMap<Integer, ZonesRow> zonesMap = hazelcastInstance.getMap("zones");
        this.localZoneIds = new HashMap<>(zonesMap); // CLiente o no? Tengo que traer los nombres para ponerlo en el PAIR?
    }

    @Override
    public void map(Integer integer, TripRow tripRow, Context<PickupCompanyPair, Double> context) {
        int PULocationID = tripRow.getPULocationID();
        String company = tripRow.getCompany();

        if ( localZoneIds.get(PULocationID) != null  ){
            String pickupLocation = localZoneIds.get(PULocationID).getBorogh();
            context.emit(new PickupCompanyPair(pickupLocation, company),tripRow.getBase_fare());
        }
    }
}
