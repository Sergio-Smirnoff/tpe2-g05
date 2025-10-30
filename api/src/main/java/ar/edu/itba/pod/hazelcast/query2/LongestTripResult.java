package ar.edu.itba.pod.hazelcast.query2;


public record LongestTripResult(String pickUpZone, String dropOffZone, String requestTime, double totalMiles, String company) implements Comparable<LongestTripResult> {

    @Override
    public String toString() {
        String truncatedMiles = String.format("%.2f", Math.floor(totalMiles * 100) / 100);

        return String.format("%s;%s;%s;%s;%s",
                pickUpZone,
                dropOffZone,
                requestTime,
                truncatedMiles.replace(",", "."),
                company);
    }

    @Override
    public int compareTo(LongestTripResult o) {

        return this.pickUpZone.compareTo(o.pickUpZone);
    }
}