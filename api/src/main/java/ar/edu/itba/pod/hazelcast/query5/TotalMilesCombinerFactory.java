package ar.edu.itba.pod.hazelcast.query5;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class TotalMilesCombinerFactory implements CombinerFactory<TotalMilesKey, Double, Double> {

    @Override
    public Combiner<Double, Double> newCombiner(TotalMilesKey totalMilesKey) {
        return new TotalMilesCombiner();
    }

    private static class TotalMilesCombiner extends Combiner<Double, Double> {
        private Double sum = 0D;

        @Override
        public void reset() {
            sum = 0D;
        }

        @Override
        public void combine(Double value) {
            sum += value;
        }

        @Override
        public Double finalizeChunk() {
            return sum;
        }

    }
}
