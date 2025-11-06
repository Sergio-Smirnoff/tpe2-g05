package ar.edu.itba.pod.hazelcast.query2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class TripRowQ2 implements DataSerializable {

    private String PULocation;
    private String DOLocation;
    private double trip_miles;
    private String company;
    private LocalDateTime requestTime;

    public TripRowQ2(){}

    public TripRowQ2(String PULocation, String DOLocation, double trip_miles, String company, LocalDateTime requestTime) {
        this.PULocation = PULocation;
        this.DOLocation = DOLocation;
        this.trip_miles = trip_miles;
        this.company = company;
        this.requestTime = requestTime;
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

    public String getCompany() {return company;}
    public LocalDateTime getRequestTime() {return requestTime;}
    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(PULocation);
        objectDataOutput.writeUTF(DOLocation);
        objectDataOutput.writeDouble(trip_miles);
        objectDataOutput.writeUTF(company);
        objectDataOutput.writeObject(requestTime);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        PULocation = objectDataInput.readUTF();
        DOLocation = objectDataInput.readUTF();
        trip_miles = objectDataInput.readDouble();
        company = objectDataInput.readUTF();
        requestTime = objectDataInput.readObject();
    }
}
