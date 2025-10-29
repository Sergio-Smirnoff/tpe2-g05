package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.Pair;
import ar.edu.itba.pod.hazelcast.common.TripRowQuery4_2;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DelayPerBoroughZoneMapper2 implements Mapper<Integer, TripRowQuery4_2, String, Pair<String, Long>> {

    @Override
    public void map(Integer integer, TripRowQuery4_2 tripRowQuery42, Context<String, Pair<String, Long>> context) {
        context.emit(tripRowQuery42.getPUZone(), new Pair<>(tripRowQuery42.getDOZone(), tripRowQuery42.getDelayInSeconds()));
    }
}
