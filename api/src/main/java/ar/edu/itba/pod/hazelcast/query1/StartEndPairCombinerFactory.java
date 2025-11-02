package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;


public class StartEndPairCombinerFactory implements CombinerFactory<Pair<String,String>, Long, Long> {

    @Override
    public Combiner<Long, Long> newCombiner(Pair key) {
        return new StartEndPairCombiner();
    }
    private static class StartEndPairCombiner extends Combiner<Long, Long> {

        private long sum = 0L;

        @Override
        public void reset() {
            sum = 0L;
        }

        @Override
        public void combine(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }
    }
}