package ar.edu.itba.pod.hazelcast.query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@Deprecated
public class PriceAvgMapper implements Mapper<Integer, TripRowQ3, PickupCompanyPair, Double>{

    @Override
    public void map(Integer integer, TripRowQ3 tripRow, Context<PickupCompanyPair, Double> context) {
        context.emit(new PickupCompanyPair(tripRow.getBorough(), tripRow.getCompany()), tripRow.getFare());
    }
}