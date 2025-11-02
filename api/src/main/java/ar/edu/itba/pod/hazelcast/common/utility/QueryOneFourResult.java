package ar.edu.itba.pod.hazelcast.common.utility;

public record QueryOneFourResult(String startZone, String endZone, Long amount) {
    @Override
    public String toString() {
        return "%s;%s;%d".formatted(startZone,endZone,amount);
    }
}

