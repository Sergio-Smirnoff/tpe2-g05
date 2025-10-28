package ar.edu.itba.pod.hazelcast.query5;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import ar.edu.itba.pod.hazelcast.query1.StartEndPair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Set;

public class TotalMilesMapper implements Mapper<Integer, TripRow, TotalMilesKey, Double> {
    private static final Long ONE = 1L;

    @Override
    public void map(Integer integer, TripRow tripRow, Context<TotalMilesKey, Double> context) {
        TotalMilesKey key = new TotalMilesKey(tripRow.getCompany(), tripRow.getRequest_time().getYear(), tripRow.getRequest_time().getMonthValue());
        context.emit(key, tripRow.getTrip_miles());
    }
}
