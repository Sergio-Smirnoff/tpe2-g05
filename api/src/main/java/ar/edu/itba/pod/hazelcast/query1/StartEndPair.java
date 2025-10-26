package ar.edu.itba.pod.hazelcast.query1;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class StartEndPair implements DataSerializable {
    private int startZone, endZone;

    public StartEndPair(){}

    public StartEndPair(int startZone, int endZone) {
        this.startZone = startZone;
        this.endZone = endZone;
    }

    public int getStartZone() {
        return startZone;
    }

    public int getEndZone() {
        return endZone;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(startZone);
        out.writeInt(endZone);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.startZone = in.readInt();
        this.endZone = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        StartEndPair that = (StartEndPair) o;
        return startZone == that.startZone && endZone == that.endZone;
    }

    @Override
    public int hashCode(){
        return Objects.hash(startZone, endZone);
    }
}
