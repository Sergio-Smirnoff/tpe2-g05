package ar.edu.itba.pod.hazelcast.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

@Deprecated
public class PickupCompanyPairReducerFactory implements ReducerFactory<PickupCompanyPair, Double, Double> {

    @Override
    public Reducer<Double, Double> newReducer(PickupCompanyPair pickupCompanyPair) {
        return new PickupCompanyPairReducer();
    }

    private class PickupCompanyPairReducer extends Reducer<Double, Double>{

        private double sum = 0;
        private double appearencies = 0;

        public PickupCompanyPairReducer() {
            super();
        }

        @Override
        public void beginReduce() {
            super.beginReduce();
        }

        @Override
        public void reduce(Double value) {
            sum += value;
            appearencies++;
        }

        @Override
        public Double finalizeReduce() {
            return sum / appearencies; // returning avg
        }
    }
}
