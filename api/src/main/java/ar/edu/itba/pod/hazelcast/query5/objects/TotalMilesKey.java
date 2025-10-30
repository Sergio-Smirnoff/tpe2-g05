package ar.edu.itba.pod.hazelcast.query5.objects;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class TotalMilesKey implements DataSerializable {
    private String company;
    private int year;
    private int month;

    public TotalMilesKey() { }

    public TotalMilesKey(String company, int year, int month) {
        this.company = company;
        this.year = year;
        this.month = month;
    }

    public String getCompany() {
        return company;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TotalMilesKey totalMilesKey
                && year == totalMilesKey.year && month == totalMilesKey.month && company.equals(totalMilesKey.company);
    }

    @Override
    public int hashCode() {
        return Objects.hash(company, year, month);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(company);
        out.writeInt(year);
        out.writeInt(month);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        company = in.readUTF();
        year = in.readInt();
        month = in.readInt();
    }
}
