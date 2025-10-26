package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class TripRow implements DataSerializable {
    private String company;
    private LocalDateTime request_time;
    private LocalDateTime pickup_time;
    private LocalDateTime dropoff_time;
    private int PULocationID;
    private int DOLocationID;
    private double trip_miles;
    private double base_fare;

    public TripRow(
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
        this.request_time = request_time;
        this.pickup_time = pickup_time;
        this.dropoff_time = dropoff_time;
        this.PULocationID = PULocationID;
        this.DOLocationID = DOLocationID;
        this.trip_miles = trip_miles;
        this.base_fare = base_fare;
    }

    public String getCompany() {
        return company;
    }

    public LocalDateTime getRequest_time() {
        return request_time;
    }

    public LocalDateTime getPickup_time() {
        return pickup_time;
    }

    public LocalDateTime getDropoff_time() {
        return dropoff_time;
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
        out.writeObject(request_time);
        out.writeObject(pickup_time);
        out.writeObject(dropoff_time);
        out.writeInt(PULocationID);
        out.writeInt(DOLocationID);
        out.writeDouble(trip_miles);
        out.writeDouble(base_fare);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        company = in.readUTF();
        request_time = in.readObject();
        pickup_time = in.readObject();
        dropoff_time = in.readObject();
        PULocationID = in.readInt();
        DOLocationID = in.readInt();
        trip_miles = in.readDouble();
        base_fare = in.readDouble();
    }
}
