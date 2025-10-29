package ar.edu.itba.pod.hazelcast.query2;


public record LongestTripResult(String pickUpZone, String longestDOZone, String longestPUDateTime, double longestMiles, String longestCompany) implements Comparable<LongestTripResult> {

    @Override
    public String toString() {
        String truncatedMiles = String.format("%.2f", Math.floor(longestMiles * 100) / 100);

        return String.format("%s;%s;%s;%s;%s",
                pickUpZone,
                longestDOZone,
                longestPUDateTime,
                truncatedMiles.replace(",", "."),
                longestCompany);
    }

    @Override
    public int compareTo(LongestTripResult o) {

        return this.pickUpZone.compareTo(o.pickUpZone);
    }
}