package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class TripRowFull extends TripRow{
    private String company;
    private LocalDateTime requestTime;
    private LocalDateTime pickupTime;
    private LocalDateTime dropoffTime;
    private int PULocationID;
    private int DOLocationID;
    private double trip_miles;
    private double base_fare;

    public TripRowFull(
            String company,
            LocalDateTime request_time,
            LocalDateTime pickup_time,
            LocalDateTime dropoff_time,
            int PULocationID,
            int DOLocationID,
            double trip_miles,
            double base_fare
    ) {
        this.company = company;
        this.requestTime = request_time;
        this.pickupTime = pickup_time;
        this.dropoffTime = dropoff_time;
        this.PULocationID = PULocationID;
        this.DOLocationID = DOLocationID;
        this.trip_miles = trip_miles;
        this.base_fare = base_fare;
    }

    public String getCompany() {
        return company;
    }

    public LocalDateTime getRequestTime() {
        return requestTime;
    }

    public LocalDateTime getPickupTime() {
        return pickupTime;
    }

    public LocalDateTime getDropoffTime() {
        return dropoffTime;
    }

    public int getPULocationID() {
        return PULocationID;
    }

    public int getDOLocationID() {
        return DOLocationID;
    }

    public double getTrip_miles() {
        return trip_miles;
    }

    public double getBase_fare() {
        return base_fare;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(company);
        out.writeObject(requestTime);
        out.writeObject(pickupTime);
        out.writeObject(dropoffTime);
        out.writeInt(PULocationID);
        out.writeInt(DOLocationID);
        out.writeDouble(trip_miles);
        out.writeDouble(base_fare);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        company = in.readUTF();
        requestTime = in.readObject();
        pickupTime = in.readObject();
        dropoffTime = in.readObject();
        PULocationID = in.readInt();
        DOLocationID = in.readInt();
        trip_miles = in.readDouble();
        base_fare = in.readDouble();
    }
}
