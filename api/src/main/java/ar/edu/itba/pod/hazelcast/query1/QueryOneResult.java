package ar.edu.itba.pod.hazelcast.query1;

import java.util.Comparator;

public record QueryOneResult(String startZone, String endZone, Long count) {
    @Override
    public String toString() {
        return "%s;%s;%d".formatted(startZone,endZone,count);
    }
}
