package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.serialization.DataSerializable;

public abstract class TripRow implements DataSerializable {
    // company;request_datetime;pickup_datetime;dropoff_datetime;PULocationID;DOLocationID;trip_miles;base_passenger_fare
    public TripRow(){}

}
