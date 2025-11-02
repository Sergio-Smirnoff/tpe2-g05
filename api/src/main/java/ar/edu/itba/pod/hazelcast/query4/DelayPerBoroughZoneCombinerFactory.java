package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import ar.edu.itba.pod.hazelcast.common.utility.Utils;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class DelayPerBoroughZoneCombinerFactory implements CombinerFactory<String, Pair<String, Long>, Pair<String, Long>> {

    @Override
    public Combiner<Pair<String, Long>, Pair<String, Long>> newCombiner(String s) {
        return new DelayPerBoroughZoneCombiner();
    }

    private static class DelayPerBoroughZoneCombiner extends Combiner<Pair<String, Long>, Pair<String, Long>> {
        private Pair<String, Long> longestDelayPair = new Pair<>("", 0L);

        @Override
        public void combine(Pair<String, Long> current) {
            this.longestDelayPair = Utils.getLongerDelay(this.longestDelayPair, current);
        }

        @Override
        public Pair<String, Long> finalizeChunk() {
            return longestDelayPair;
        }
    }
}
