package ar.edu.itba.pod.hazelcast.query1;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class StartEndPairReducerFactory implements ReducerFactory<StartEndPair, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(StartEndPair key){
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
