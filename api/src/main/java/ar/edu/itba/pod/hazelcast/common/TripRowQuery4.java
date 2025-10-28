package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class TripRowQuery4 implements DataSerializable {
    private LocalDateTime requestTime;
    private LocalDateTime pickupTime;
    private int PULocationID;
    private int DOLocationID;

    public TripRowQuery4(){}

    public TripRowQuery4(
            LocalDateTime request_time,
            LocalDateTime pickup_time,
            int PULocationID,
            int DOLocationID
    ) {
        this.requestTime = request_time;
        this.pickupTime = pickup_time;
        this.PULocationID = PULocationID;
        this.DOLocationID = DOLocationID;
    }

    public LocalDateTime getRequestTime() {
        return requestTime;
    }

    public LocalDateTime getPickupTime() {
        return pickupTime;
    }

    public int getPULocationID() {
        return PULocationID;
    }

    public int getDOLocationID() {
        return DOLocationID;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(requestTime);
        out.writeObject(pickupTime);
        out.writeInt(PULocationID);
        out.writeInt(DOLocationID);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        requestTime = in.readObject();
        pickupTime = in.readObject();
        PULocationID = in.readInt();
        DOLocationID = in.readInt();
    }
}
