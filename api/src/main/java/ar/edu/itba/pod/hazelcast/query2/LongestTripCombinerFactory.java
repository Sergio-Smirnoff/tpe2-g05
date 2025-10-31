package ar.edu.itba.pod.hazelcast.query2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class LongestTripCombinerFactory implements CombinerFactory<String, LongestTripValue, LongestTripValue> {

    @Override
    public Combiner<LongestTripValue, LongestTripValue> newCombiner(String key) {
        return new LongestTripCombiner();
    }
    private static class LongestTripCombiner extends Combiner<LongestTripValue, LongestTripValue> {

        private LongestTripValue maxTrip = null;

        @Override
        public void reset() {
            maxTrip = null;
        }

        @Override
        public void combine(LongestTripValue value) {
            if (maxTrip == null || value.compareTo(maxTrip) > 0) {
                maxTrip = value;
            }
        }

        @Override
        public LongestTripValue finalizeChunk() {
            return maxTrip;
        }
    }
}