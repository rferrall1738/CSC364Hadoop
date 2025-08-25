package org.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewCombiner extends Reducer<Text, PairWritable, Text, PairWritable> {
    private final PairWritable outValue = new PairWritable();

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context)
            throws IOException, InterruptedException {
        long positive = 0, total = 0;
        for (PairWritable pw : values) {
            positive += pw.getPositive();
            total += pw.getTotal();
        }
        outValue.setPositive(positive, total);
        context.write(key, outValue);
    }
}
