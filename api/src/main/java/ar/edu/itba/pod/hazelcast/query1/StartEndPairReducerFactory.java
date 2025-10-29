package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class StartEndPairReducerFactory implements ReducerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair key){
        return new StartEndPairReducer();
    }

    private class StartEndPairReducer extends Reducer<Long, Long>{
        private long sum = 0L;

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
