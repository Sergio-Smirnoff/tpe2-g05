package ar.edu.itba.pod.hazelcast.query2;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

@Deprecated
public class LongestTripMapper implements Mapper<Integer, TripRowQ2, String, LongestTripValue>{

    public LongestTripMapper() {}

    @Override
    public void map(Integer integer, TripRowQ2 tripRow, Context<String, LongestTripValue> context) {

        context.emit(tripRow.getPULocation(), new LongestTripValue(tripRow.getTrip_miles(),
                tripRow.getRequestTime(),
                tripRow.getDOLocation(),
                tripRow.getCompany()));
    }


}
