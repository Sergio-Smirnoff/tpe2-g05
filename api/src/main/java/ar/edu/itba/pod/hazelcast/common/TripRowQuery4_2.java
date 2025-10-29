package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class TripRowQuery4_2 implements DataSerializable {
    private LocalDateTime requestTime;
    private LocalDateTime pickupTime;
    private String PUZone;
    private String DOZone;

    public TripRowQuery4_2(){}

    public TripRowQuery4_2(
            LocalDateTime request_time,
            LocalDateTime pickup_time,
            String PUZone,
            String DOZone
    ) {
        this.requestTime = request_time;
        this.pickupTime = pickup_time;
        this.PUZone = PUZone;
        this.DOZone = DOZone;
    }

    public LocalDateTime getRequestTime() {
        return requestTime;
    }

    public LocalDateTime getPickupTime() {
        return pickupTime;
    }

    public String getPUZone() {
        return PUZone;
    }

    public String getDOZone() {
        return DOZone;
    }

    public long getDelayInSeconds() {
        return ChronoUnit.SECONDS.between(requestTime, pickupTime);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(requestTime);
        out.writeObject(pickupTime);
        out.writeUTF(PUZone);
        out.writeUTF(DOZone);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        requestTime = in.readObject();
        pickupTime = in.readObject();
        PUZone = in.readUTF();
        DOZone = in.readUTF();
    }
}
