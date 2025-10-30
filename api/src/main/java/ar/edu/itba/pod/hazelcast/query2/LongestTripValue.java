package ar.edu.itba.pod.hazelcast.query2;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class LongestTripValue implements DataSerializable, Comparable<LongestTripValue> {

    private double tripMiles;
    private LocalDateTime requestTime;
    private String DOLocationID;
    private String company;

    public LongestTripValue() {}

    public LongestTripValue(double tripMiles, LocalDateTime requestTime, String DOLocationID, String company) {
        this.tripMiles = tripMiles;
        this.requestTime = requestTime;
        this.DOLocationID = DOLocationID;
        this.company = company;
    }

    public double getTripMiles() { return tripMiles; }
    public LocalDateTime getRequestTime() { return requestTime; }
    public String getDOLocationID() { return DOLocationID; }
    public String getCompany() { return company; }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeDouble(tripMiles);
        out.writeObject(requestTime); // LocalDateTime es serializable por defecto
        out.writeUTF(DOLocationID);
        out.writeUTF(company);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        tripMiles = in.readDouble();
        requestTime = in.readObject();
        DOLocationID = in.readUTF();
        company = in.readUTF();
    }

    @Override
    public int compareTo(LongestTripValue o) {
        int mileComparison = Double.compare(this.tripMiles, o.tripMiles);

        if (mileComparison != 0) {
            return mileComparison;
        }
        return this.requestTime.compareTo(o.requestTime);
    }
}
