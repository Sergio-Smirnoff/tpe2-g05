package ar.edu.itba.pod.hazelcast.query2;

import ar.edu.itba.pod.hazelcast.common.ZonesRow;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class LongestTripCollator implements Collator<Map.Entry<Integer, LongestTripValue>, SortedSet<LongestTripResult>> {


    private final IMap<Integer, ZonesRow> zonesMap;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    public LongestTripCollator(IMap<Integer, ZonesRow> zonesMap) {
        this.zonesMap = zonesMap;
    }

    @Override
    public SortedSet<LongestTripResult> collate(Iterable<Map.Entry<Integer, LongestTripValue>> values) {

        Map<Integer, ZonesRow> localZonesMap = new HashMap<>(this.zonesMap);

        SortedSet<LongestTripResult> results = new TreeSet<>();

        for (Map.Entry<Integer, LongestTripValue> entry : values) {
            int PULocationID = entry.getKey();
            LongestTripValue maxTrip = entry.getValue();

            // Evitar NullPointerException si una zona no existe
            ZonesRow pickUpZoneRow = localZonesMap.get(PULocationID);
            ZonesRow dropOffZoneRow = localZonesMap.get(maxTrip.getDOLocationID());

            if (pickUpZoneRow == null || dropOffZoneRow == null) {
                continue; // O loggear un warning
            }

            String pickUpZoneName = pickUpZoneRow.getZone();
            String dropOffZoneName = dropOffZoneRow.getZone();

            LongestTripResult result = new LongestTripResult(
                    pickUpZoneName,
                    dropOffZoneName,
                    maxTrip.getRequestTime().format(DATE_FORMATTER),
                    maxTrip.getTripMiles(),
                    maxTrip.getCompany()
            );
            results.add(result);
        }
        return results;
    }
}
