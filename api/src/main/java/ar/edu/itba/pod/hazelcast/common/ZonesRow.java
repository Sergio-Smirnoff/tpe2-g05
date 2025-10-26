package ar.edu.itba.pod.hazelcast.common;

public class ZonesRow {

    private int LocationID;
    private String Borogh;
    private String Zone;

    public ZonesRow(int locationID, String borogh, String zone) {
        LocationID = locationID;
        Borogh = borogh;
        Zone = zone;
    }

    public int getLocationID() {
        return LocationID;
    }

    public String getBorogh() {
        return Borogh;
    }

    public String getZone() {
        return Zone;
    }
}
