package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class DelayPerBoroughZoneReducerFactory implements ReducerFactory<String, Pair<String, Long>, Pair<String, Long>> {

    @Override
    public Reducer<Pair<String, Long>, Pair<String, Long>> newReducer(String s) {
        return new DelayPerBoroughZoneReducer();
    }

    private class DelayPerBoroughZoneReducer extends Reducer<Pair<String, Long>, Pair<String, Long>>{
        private Pair<String, Long> longestDelayPair = new Pair<>("", 0L);

        @Override
        public void reduce(Pair<String, Long> current) {

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
        public Pair<String, Long> finalizeReduce() {
            return longestDelayPair;
        }
    }
}
