package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DelayPerBoroughZoneMapper implements Mapper<Integer, TripRowQ4, String, Pair<String, Long>> {

    @Override
    public void map(Integer integer, TripRowQ4 tripRowQ4, Context<String, Pair<String, Long>> context) {
        context.emit(tripRowQ4.getPUZone(), new Pair<>(tripRowQ4.getDOZone(), tripRowQ4.getDelayInSeconds()));
    }
}
