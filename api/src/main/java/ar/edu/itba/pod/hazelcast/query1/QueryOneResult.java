package ar.edu.itba.pod.hazelcast.query1;

import java.util.Comparator;

public class QueryOneResult {
    private final String startZone;
    private final String endZone;
    private final Long count;

    public QueryOneResult(String startzone, String endZone, Long count){
        this.startZone = startzone;
        this.endZone = endZone;
        this.count = count;
    }

    public String getStartZone(){
        return this.startZone;
    }

    public String getEndZone(){
        return this.endZone;
    }

    public Long getCount(){
        return this.count;
    }

    public static Comparator<QueryOneResult> getComparator() {
        return Comparator.comparing(QueryOneResult::getCount).reversed()
                .thenComparing(QueryOneResult::getStartZone)
                .thenComparing(QueryOneResult::getEndZone);
    }

    public String toCsvLine() {
        return startZone + ";" + endZone + ";" + count;
    }
}
