package org.example;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        String[] a = args;
        if (a.length >= 3 && "org.example.Main".equals(a[0])) {
            a = Arrays.copyOfRange(a, 1, a.length);
        }

        if (a.length != 2) {
            System.err.println("Usage: <input path> <output path>");
            System.exit(2);
        }
        String in  = a[0];
        String out = a[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Steam Reviews");

        job.setJarByClass(Main.class);
        job.setMapperClass(ReviewMapper.class);
        job.setCombinerClass(ReviewCombiner.class);
        job.setReducerClass(ReviewReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
