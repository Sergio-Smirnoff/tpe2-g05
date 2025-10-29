package ar.edu.itba.pod.hazelcast.query5;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TotalMilesCollator implements Collator<Map.Entry<TotalMilesKey, Double>, List<Map.Entry<TotalMilesKey, Double>>> {
    @Override
    public List<Map.Entry<TotalMilesKey, Double>> collate(Iterable<Map.Entry<TotalMilesKey, Double>> iterable) {

        // --------------- SORT ----------------------
        List<Map.Entry<TotalMilesKey, Double>> result = StreamSupport.stream(iterable.spliterator(), false)
                .sorted(Comparator.comparing((Map.Entry<TotalMilesKey, Double> e) -> e.getKey().company())
                        .thenComparing(e -> e.getKey().year())
                        .thenComparing(e -> e.getKey().month()))
                .toList();

        // ---------------- YTD SUM --------------------
        String previousCompany = null;
        Integer actualYear = null;
        Double previousTotal = null;

        for (Map.Entry<TotalMilesKey, Double> e : result) {
            String company = e.getKey().company();
            if(previousCompany == null || !previousCompany.equals(company) || actualYear != e.getKey().year()) {
                previousCompany = company;
                actualYear = e.getKey().year();
                previousTotal = 0.0;
            }

            double totalYTD = previousTotal + e.getValue();

            double truncatedValue = Math.floor(totalYTD * 100) / 100;
            e.setValue(truncatedValue);

            previousTotal = totalYTD;
        }

        return result;
    }
}
