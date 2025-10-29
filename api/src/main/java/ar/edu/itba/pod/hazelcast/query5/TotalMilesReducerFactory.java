package ar.edu.itba.pod.hazelcast.query5;

import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesKey;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class TotalMilesReducerFactory implements ReducerFactory<TotalMilesKey, Double, Double> {

    @Override
    public Reducer<Double, Double> newReducer(TotalMilesKey startEndPair) {
        return new TotalMilesReducer();
    }

    private static class TotalMilesReducer extends Reducer<Double, Double>{
        private Double sum = 0D;

        @Override
        public void reduce(Double value) {
            sum += value;
        }

        @Override
        public Double finalizeReduce() {
            return sum;
        }
    }
}
