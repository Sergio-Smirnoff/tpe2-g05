package ar.edu.itba.pod.hazelcast.query1;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TripRowQ1 implements DataSerializable {

    private String PULocation;
    private String DOLocation;


    public TripRowQ1(){}

    public TripRowQ1(String PULocation, String DOLocation) {
        this.PULocation = PULocation;
        this.DOLocation = DOLocation;
    }

    public String getPULocation() {
        return PULocation;
    }

    public String getDOLocation() {
        return DOLocation;
    }


    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        PULocation = objectDataInput.readUTF();
        DOLocation = objectDataInput.readUTF();
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(PULocation);
        objectDataOutput.writeUTF(DOLocation);
    }
}
