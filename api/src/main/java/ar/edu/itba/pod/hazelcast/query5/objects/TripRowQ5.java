package ar.edu.itba.pod.hazelcast.query5.objects;

import ar.edu.itba.pod.hazelcast.common.TripRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.time.LocalDateTime;

public class TripRowQ5 extends TripRow {

    private String company;
    private LocalDateTime requestTime;
    private double trip_miles;

    public TripRowQ5(){}

    public TripRowQ5(String company, LocalDateTime requestTime, double trip_miles) {
        this.company = company;
        this.requestTime = requestTime;
        this.trip_miles = trip_miles;
    }

    public String getCompany() {
        return company;
    }

    public LocalDateTime getRequestTime() {
        return requestTime;
    }

    public double getTrip_miles() {
        return trip_miles;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(company);
        objectDataOutput.writeObject(requestTime);
        objectDataOutput.writeDouble(trip_miles);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        company = objectDataInput.readUTF();
        requestTime = objectDataInput.readObject();
        trip_miles = objectDataInput.readDouble();

    }
}
