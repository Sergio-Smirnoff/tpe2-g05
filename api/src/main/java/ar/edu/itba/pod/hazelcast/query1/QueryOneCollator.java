package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class QueryOneCollator implements Collator<Map.Entry<StartEndPair, Long>, SortedSet<QueryOneResult>> {

    private IMap<Integer, ZonesRow> zonesMap;

    public QueryOneCollator(IMap<Integer, ZonesRow> zonesMap) {
        this.zonesMap = zonesMap;
    }

    @Override
    public SortedSet<QueryOneResult> collate(Iterable<Map.Entry<StartEndPair, Long>> iterable) {
        Map<Integer, ZonesRow> localZonesMap = new HashMap<>(this.zonesMap);

        SortedSet<QueryOneResult> toReturn = new TreeSet<>(
                Comparator.comparing(QueryOneResult::count).reversed()
                        .thenComparing(QueryOneResult::startZone)
                        .thenComparing(QueryOneResult::endZone)
        );

        for (Map.Entry<StartEndPair, Long> entry : iterable) {
            String startZone = localZonesMap.get(entry.getKey().getStartZone()).getZone();
            String endZone = localZonesMap.get(entry.getKey().getEndZone()).getZone();
            toReturn.add(new QueryOneResult(
                    startZone,
                    endZone,
                    entry.getValue()
            ));
        }

        return toReturn;
    }
}
