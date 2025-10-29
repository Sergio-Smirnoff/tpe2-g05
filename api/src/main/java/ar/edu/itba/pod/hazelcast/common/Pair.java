package ar.edu.itba.pod.hazelcast.common;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class Pair<T, S> implements DataSerializable {
    private T left;
    private S right;

    public Pair(){}

    public Pair(T left, S right) {
        this.left = left;
        this.right = right;
    }

    public void setLeft(T left) {
        this.left = left;
    }

    public void setRight(S right) {
        this.right = right;
    }

    public T getLeft() {
        return left;
    }

    public S getRight() {
        return right;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.left = in.readObject();
        this.right = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        Pair<?,?> that = (Pair<?,?>) o;
        return left.equals(that.left) && right.equals(that.right);
    }

    @Override
    public int hashCode(){
        return Objects.hash(left, right);
    }
}
