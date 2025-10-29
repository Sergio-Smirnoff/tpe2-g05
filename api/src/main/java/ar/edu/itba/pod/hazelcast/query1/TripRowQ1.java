package ar.edu.itba.pod.hazelcast.query1;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.time.LocalDateTime;

public class TripRowQ1 extends TripRow {

    private String PULocation;
    private String DOLocation;
    private double trip_miles;


    public TripRowQ1(){}

    public TripRowQ1(String PULocation, String DOLocation, double trip_miles) {
        this.PULocation = PULocation;
        this.DOLocation = DOLocation;
        this.trip_miles = trip_miles;
    }

    public String getPULocation() {
        return PULocation;
    }

    public String getDOLocation() {
        return DOLocation;
    }

    public double getTrip_miles() {
        return trip_miles;
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        PULocation = objectDataInput.readUTF();
        DOLocation = objectDataInput.readUTF();
        trip_miles = objectDataInput.readDouble();
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(PULocation);
        objectDataOutput.writeUTF(DOLocation);
        objectDataOutput.writeDouble(trip_miles);
    }
}
