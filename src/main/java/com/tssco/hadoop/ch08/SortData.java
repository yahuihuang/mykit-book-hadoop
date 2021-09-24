package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SortData implements WritableComparable<SortData> {
    private Long first;
    private Long second;

    public SortData() {
    }

    public SortData(Long first, Long second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(SortData o) {
        final Long minus = this.first - o.getFirst();
        if (minus != 0) {
            return minus.intValue();
        }
        return (int)(this.second - o.getSecond());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(first);
        dataOutput.writeLong(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readLong();
        this.second = dataInput.readLong();
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof SortData) != true) {
            return false;
        }

        SortData sortData = (SortData) obj;
        return Objects.equals(first, sortData.first) && Objects.equals(second, sortData.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    public Long getFirst() {
        return first;
    }

    public void setFirst(Long first) {
        this.first = first;
    }

    public Long getSecond() {
        return second;
    }

    public void setSecond(Long second) {
        this.second = second;
    }
}
