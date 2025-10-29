package ar.edu.itba.pod.hazelcast.query5.objects;


// Doesnt need Serializable because its used only in Collator (Client-side)
public record TotalMilesResult(String company, int year, int month, double totalMiles) {
    @Override
    public String toString() {
        return "%s,%d,%d,%.2f".formatted(company, year, month, totalMiles);
    }
}
