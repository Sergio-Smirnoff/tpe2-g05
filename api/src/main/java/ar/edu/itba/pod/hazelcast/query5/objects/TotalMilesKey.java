package ar.edu.itba.pod.hazelcast.query5.objects;

import java.io.Serializable;
import java.util.Objects;

public record TotalMilesKey (String company, int year, int month) implements Serializable {
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TotalMilesKey that = (TotalMilesKey) o;
        return year == that.year && month == that.month && Objects.equals(company, that.company);
    }

    @Override
    public int hashCode() {
        return Objects.hash(company, year, month);
    }
}
