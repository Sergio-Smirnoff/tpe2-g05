package ar.edu.itba.pod.hazelcast.query5;

import ar.edu.itba.pod.hazelcast.common.TripRow;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class TotalMilesMapper implements Mapper<Integer, TripRow, TotalMilesKey, Double> {
    @Override
    public void map(Integer integer, TripRow tripRow, Context<TotalMilesKey, Double> context) {
        TotalMilesKey key = new TotalMilesKey(tripRow.getCompany(), tripRow.getRequestTime().getYear(), tripRow.getRequestTime().getMonthValue());
        context.emit(key, tripRow.getTrip_miles());
    }
}
