package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;


public class StartEndPairMapper implements Mapper<Integer, TripRowQ1, Pair<String, String>, Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(Integer integer, TripRowQ1 tripRow, Context<Pair<String, String>, Long> context) {
        context.emit(new Pair<>(tripRow.getPULocation(), tripRow.getDOLocation()), ONE);
    }
}
