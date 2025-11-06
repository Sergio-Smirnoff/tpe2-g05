package ar.edu.itba.pod.hazelcast.query3;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TripRowQ3 implements DataSerializable {

    private String borough;
    private String company;
    private double fare;

    public  TripRowQ3(){}

    public TripRowQ3(String borough, String company, double fare) {
        this.borough = borough;
        this.company = company;
        this.fare = fare;
    }

    public String getBorough() {
        return borough;
    }

    public String getCompany() {
        return company;
    }

    public double getFare() {
        return fare;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(company);
        objectDataOutput.writeUTF(borough);
        objectDataOutput.writeDouble(fare);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        company = objectDataInput.readUTF();
        borough = objectDataInput.readUTF();
        fare = objectDataInput.readDouble();
    }
}
