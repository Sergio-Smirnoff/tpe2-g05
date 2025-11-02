package ar.edu.itba.pod.hazelcast.query2;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class LongestTripCollator implements Collator<Map.Entry<String, LongestTripValue>, SortedSet<LongestTripResult>> {


    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    @Override
    public SortedSet<LongestTripResult> collate(Iterable<Map.Entry<String, LongestTripValue>> values) {

        SortedSet<LongestTripResult> results = new TreeSet<>();

        for (Map.Entry<String, LongestTripValue> entry : values) {
            String PULocation = entry.getKey();
            LongestTripValue maxTrip = entry.getValue();

            LongestTripResult result = new LongestTripResult(
                    PULocation,
                    maxTrip.getDOLocation(),
                    maxTrip.getRequestTime().format(DATE_FORMATTER),
                    maxTrip.getTripMiles(),
                    maxTrip.getCompany()
            );
            results.add(result);
        }
        return results;
    }
}
