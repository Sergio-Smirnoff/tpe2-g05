package ar.edu.itba.pod.hazelcast.query3;

import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query3Collator implements Collator<Map.Entry<PickupCompanyPair, Double>, List<AvgPriceBoroughCompany>> {
    @Override
    public List<AvgPriceBoroughCompany> collate(Iterable<Map.Entry<PickupCompanyPair, Double>> values) {
        return StreamSupport.stream(
                values.spliterator(),false
        ).sorted(
                Comparator.comparingDouble((Map.Entry<PickupCompanyPair, Double> t) -> t.getValue()).reversed().thenComparing(Map.Entry::getKey)
        ).map(entry ->
                new AvgPriceBoroughCompany(entry.getKey().getPULocation(),entry.getKey().getCompany(), entry.getValue())
        ).collect(Collectors.toList());
    }
}
