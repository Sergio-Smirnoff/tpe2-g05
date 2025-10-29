package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class ZonesRow implements DataSerializable {

    private int LocationID;
    private String Borough;
    private String Zone;

    public ZonesRow(){}

    public ZonesRow(int locationID, String borogh, String zone) {
        LocationID = locationID;
        Borough = borogh;
        Zone = zone;
    }

    public int getLocationID() {
        return LocationID;
    }

    public String getBorough() {
        return Borough;
    }

    public String getZone() {
        return Zone;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(LocationID);
        out.writeUTF(Borough);
        out.writeUTF(Zone);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        LocationID = in.readInt();
        Borough = in.readUTF();
        Zone = in.readUTF();
    }
}
