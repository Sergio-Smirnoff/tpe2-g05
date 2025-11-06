package ar.edu.itba.pod.hazelcast.query4;

import ar.edu.itba.pod.hazelcast.common.utility.Pair;
import ar.edu.itba.pod.hazelcast.common.utility.QueryOneFourResult;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

@Deprecated
public class DelayPerBoroughZoneCollator implements Collator<Map.Entry<String, Pair<String, Long>>, SortedSet<QueryOneFourResult>> {

    @Override
    public SortedSet<QueryOneFourResult> collate(Iterable<Map.Entry<String, Pair<String, Long>>> iterable) {
        SortedSet<QueryOneFourResult> toReturn = new TreeSet<>(
                Comparator.comparing(QueryOneFourResult::startZone)
        );

        for (Map.Entry<String, Pair<String, Long>> entry : iterable){
            toReturn.add(new QueryOneFourResult(
                    entry.getKey(),
                    entry.getValue().getLeft(),
                    entry.getValue().getRight()
            ));
        }

        return toReturn;
    }

}
