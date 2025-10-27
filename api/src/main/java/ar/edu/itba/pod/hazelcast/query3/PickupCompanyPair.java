package ar.edu.itba.pod.hazelcast.query3;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

public class PickupCompanyPair implements DataSerializable, Comparable<PickupCompanyPair> {
    private String PULocation;
    private String company;


    public PickupCompanyPair() {
    }

    public PickupCompanyPair(String PULocation, String company) {
        this.PULocation = PULocation;
        this.company = company;
    }

    public String getPULocation() {
        return PULocation;
    }

    public String getCompany() {
        return company;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(PULocation);
        objectDataOutput.writeUTF(company);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        PULocation = objectDataInput.readUTF();
        company = objectDataInput.readUTF();
    }

    @Override
    public int hashCode() {
        return Objects.hash(PULocation, company);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PickupCompanyPair that = (PickupCompanyPair) o;
        return Objects.equals(PULocation,that.PULocation) && Objects.equals(company, that.company);
    }
    @Override
    public String toString() {
        return "PickupCompanyPair{" +
                "pickupid=" + PULocation +
                ", company='" + company + '\'' +
                '}';
    }

    @Override
    public int compareTo(PickupCompanyPair pickupCompanyPair) {
        int result = pickupCompanyPair.getPULocation().compareTo(this.getPULocation());
        if (result != 0)
            return result;
        return pickupCompanyPair.getCompany().compareTo(this.getCompany()) ;    }
}