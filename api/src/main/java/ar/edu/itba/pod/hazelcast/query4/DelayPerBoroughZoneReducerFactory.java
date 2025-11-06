package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import ar.edu.itba.pod.hazelcast.common.utility.Utils;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@Deprecated
public class DelayPerBoroughZoneReducerFactory implements ReducerFactory<String, Pair<String, Long>, Pair<String, Long>> {

    @Override
    public Reducer<Pair<String, Long>, Pair<String, Long>> newReducer(String s) {
        return new DelayPerBoroughZoneReducer();
    }

    private class DelayPerBoroughZoneReducer extends Reducer<Pair<String, Long>, Pair<String, Long>>{
        private Pair<String, Long> longestDelayPair = new Pair<>("", 0L);

        @Override
        public void reduce(Pair<String, Long> current) {
            this.longestDelayPair = Utils.getLongerDelay(this.longestDelayPair, current);
        }

        @Override
        public Pair<String, Long> finalizeReduce() {
            return longestDelayPair;
        }
    }
}
