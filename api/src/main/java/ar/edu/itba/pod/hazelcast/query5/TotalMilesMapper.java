package ar.edu.itba.pod.hazelcast.query5;

import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesKey;
import ar.edu.itba.pod.hazelcast.query5.objects.TripRowQ5;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@Deprecated
public class TotalMilesMapper implements Mapper<Integer, TripRowQ5, TotalMilesKey, Double> {
    @Override
    public void map(Integer integer, TripRowQ5 tripRow, Context<TotalMilesKey, Double> context) {
        TotalMilesKey key = new TotalMilesKey(tripRow.getCompany(), tripRow.getRequestTime().getYear(), tripRow.getRequestTime().getMonthValue());
        context.emit(key, tripRow.getTrip_miles());
    }
}
