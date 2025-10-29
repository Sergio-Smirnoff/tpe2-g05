package ar.edu.itba.pod.hazelcast.query5;

import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesKey;
import ar.edu.itba.pod.hazelcast.query5.objects.TotalMilesResult;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.StreamSupport;

public class TotalMilesCollator implements Collator<Map.Entry<TotalMilesKey, Double>, List<TotalMilesResult>> {
    @Override
    public List<TotalMilesResult> collate(Iterable<Map.Entry<TotalMilesKey, Double>> iterable) {

        // --------------- SORT ----------------------
        List<Map.Entry<TotalMilesKey, Double>> ordered = StreamSupport.stream(iterable.spliterator(), false)
                .sorted(Comparator.comparing((Map.Entry<TotalMilesKey, Double> e) -> e.getKey().company())
                        .thenComparing(e -> e.getKey().year())
                        .thenComparing(e -> e.getKey().month()))
                .toList();

        // ---------------- YTD SUM --------------------
        String previousCompany = null;
        Integer previousYear = null;
        Double previousTotal = null;

        List<TotalMilesResult> result = new ArrayList<>();

        for (Map.Entry<TotalMilesKey, Double> e : ordered) {
            String actualCompany = e.getKey().company();
            int actualYear = e.getKey().year();
            if(previousCompany == null || !previousCompany.equals(actualCompany) || previousYear != actualYear)
                previousTotal = 0.0;

            double totalYTD = previousTotal + e.getValue();
            double truncatedValue = Math.floor(totalYTD * 100) / 100;
            result.add(new TotalMilesResult(actualCompany, actualYear, e.getKey().month(), truncatedValue));

            // Update previous values
            previousTotal = totalYTD;
            previousCompany = actualCompany;
            previousYear = actualYear;
        }

        return result;
    }
}
