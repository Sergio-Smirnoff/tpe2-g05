package ar.edu.itba.pod.hazelcast.query2;


import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class LongestTripReducerFactory implements ReducerFactory<Integer, LongestTripValue, LongestTripValue> {

    @Override
    public Reducer<LongestTripValue, LongestTripValue> newReducer(Integer key) {
        // La 'key' es el PULocationID
        return new LongestTripReducer();
    }

    private static class LongestTripReducer extends Reducer<LongestTripValue, LongestTripValue> {

        private LongestTripValue maxTripValue;

        @Override
        public void beginReduce() {
            maxTripValue = null;
        }

        @Override
        public void reduce(LongestTripValue value) {
            if (maxTripValue == null || value.compareTo(maxTripValue) > 0) {
                maxTripValue = value;
            }
        }

        @Override
        public LongestTripValue finalizeReduce() {
            return maxTripValue;
        }
    }
}