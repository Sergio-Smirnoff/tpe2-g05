package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class QueryOneCollator implements Collator<Map.Entry<Pair<String, String>, Long>, SortedSet<QueryOneFourResult>> {

    @Override
    public SortedSet<QueryOneFourResult> collate(Iterable<Map.Entry<Pair<String, String>, Long>> iterable) {

        SortedSet<QueryOneFourResult> toReturn = new TreeSet<>(
                Comparator.comparing(QueryOneFourResult::amount).reversed()
                        .thenComparing(QueryOneFourResult::startZone)
                        .thenComparing(QueryOneFourResult::endZone)
        );

        for (Map.Entry<Pair<String, String>, Long> entry : iterable) {
            toReturn.add(new QueryOneFourResult(
                    entry.getKey().getLeft(),
                    entry.getKey().getRight(),
                    entry.getValue()
            ));
        }

        return toReturn;
    }
}
