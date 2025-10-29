package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.QueryOneFourResult;
import ar.edu.itba.pod.hazelcast.common.Pair;
import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class QueryOneCollator implements Collator<Map.Entry<Pair<Integer, Integer>, Long>, SortedSet<QueryOneFourResult>> {

    private IMap<Integer, ZonesRow> zonesMap;

    public QueryOneCollator(IMap<Integer, ZonesRow> zonesMap) {
        this.zonesMap = zonesMap;
    }

    @Override
    public SortedSet<QueryOneFourResult> collate(Iterable<Map.Entry<Pair<Integer, Integer>, Long>> iterable) {
        Map<Integer, ZonesRow> localZonesMap = new HashMap<>(this.zonesMap);

        SortedSet<QueryOneFourResult> toReturn = new TreeSet<>(
                Comparator.comparing(QueryOneFourResult::amount).reversed()
                        .thenComparing(QueryOneFourResult::startZone)
                        .thenComparing(QueryOneFourResult::endZone)
        );

        for (Map.Entry<Pair<Integer, Integer>, Long> entry : iterable) {
            String startZone = localZonesMap.get(entry.getKey().getLeft()).getZone();
            String endZone = localZonesMap.get(entry.getKey().getRight()).getZone();
            toReturn.add(new QueryOneFourResult(
                    startZone,
                    endZone,
                    entry.getValue()
            ));
        }

        return toReturn;
    }
}
