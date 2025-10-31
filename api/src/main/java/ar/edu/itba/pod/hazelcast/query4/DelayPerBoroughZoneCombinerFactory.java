package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.Pair;
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
            long currentDelay = current.getRight();
            String currentZone = current.getLeft();
            long maxDelay = longestDelayPair.getRight();
            String maxZone = longestDelayPair.getLeft();

            if (currentDelay > maxDelay) {
                longestDelayPair = new Pair<>(currentZone, currentDelay);
            } else if (currentDelay == maxDelay) {
                if (currentZone.compareTo(maxZone) < 0) {
                    longestDelayPair = new Pair<>(currentZone, currentDelay);
                }
            }
        }

        @Override
        public Pair<String, Long> finalizeChunk() {
            return longestDelayPair;
        }
    }
}
