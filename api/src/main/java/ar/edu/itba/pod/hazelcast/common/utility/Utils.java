package ar.edu.itba.pod.hazelcast.common.utility;

public class Utils {

    public static Pair<String, Long> getLongerDelay(Pair<String, Long> currentMax, Pair<String, Long> newTrip) {
        long currentDelay = newTrip.getRight();
        String currentZone = newTrip.getLeft();
        long maxDelay = currentMax.getRight();
        String maxZone = currentMax.getLeft();

        if (currentDelay > maxDelay) {
            return newTrip; // New trip is the max
        } else if (currentDelay == maxDelay) {
            if (currentZone.compareTo(maxZone) < 0) {
                return newTrip; // New trip wins tie-breaker
            }
        }
        return currentMax; // Old max remains
    }

}
