package org.example;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewReducer extends Reducer<Text, PairWritable, Text, Text> {
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context)
            throws IOException, InterruptedException {
        long positive = 0;
        long total = 0;

        for (PairWritable value : values) {
            positive += value.getPositive();
            total += value.getTotal();
        }

        double ratio = 0.0;
        if (total > 0) ratio = positive / (double) total;
        outValue.set(positive + "\t" + total +"\t" +ratio);
        context.write(key, outValue);
    }
}
