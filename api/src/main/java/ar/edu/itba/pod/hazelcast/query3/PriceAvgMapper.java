package ar.edu.itba.pod.hazelcast.query3;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class PriceAvgMapper implements Mapper<Integer, TripRow, PickupCompanyPair, Double>, HazelcastInstanceAware {
    private transient IMap<Integer, ZonesRow> zonesMap;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.zonesMap = hazelcastInstance.getMap("zones");
    }

    @Override
    public void map(Integer integer, TripRow tripRow, Context<PickupCompanyPair, Double> context) {
        int PULocationID = tripRow.getPULocationID();
        String company = tripRow.getCompany();

        ZonesRow zone = zonesMap.get(PULocationID);

        if (zone != null) {
            String pickupBorough = zone.getBorogh(); // Usar getBorogh()
            context.emit(new PickupCompanyPair(pickupBorough, company), tripRow.getBase_fare());
        }
    }
}