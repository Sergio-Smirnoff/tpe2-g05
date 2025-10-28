package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class QueryOneCollator implements Collator<Map.Entry<StartEndPair, Long>, SortedSet<QueryOneResult>> {

    private IMap<Integer, ZonesRow> zonesMap;

    public QueryOneCollator(IMap<Integer, ZonesRow> zonesMap) {
        this.zonesMap = zonesMap;
    }

    @Override
    public SortedSet<QueryOneResult> collate(Iterable<Map.Entry<StartEndPair, Long>> iterable) {
        SortedSet<QueryOneResult> toReturn = new TreeSet<>(
                Comparator.comparing(QueryOneResult::count).reversed()
                        .thenComparing(QueryOneResult::startZone)
                        .thenComparing(QueryOneResult::endZone)
        );

        for (Map.Entry<StartEndPair, Long> entry : iterable) {
            String startZone = zonesMap.get(entry.getKey().getStartZone()).getZone();
            String endZone = zonesMap.get(entry.getKey().getEndZone()).getZone();
            toReturn.add(new QueryOneResult(
                    startZone,
                    endZone,
                    entry.getValue()
            ));
        }

        return toReturn;
    }
}
