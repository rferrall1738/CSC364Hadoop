package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable {

    private long positive;
    private long total;

    public PairWritable() {}

    public PairWritable(long positive, long total) {
        this.positive = positive;
        this.total = total;
    }

    public long getPositive() { return positive; }
    public long getTotal()    { return total;    }

    public void setPositive(long positive, long total) {
        this.positive = positive;
        this.total = total;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        positive = in.readLong();
        total = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(positive);
        out.writeLong(total);
    }

    @Override
    public String toString() {
        return positive + "\t" + total;
    }
}
