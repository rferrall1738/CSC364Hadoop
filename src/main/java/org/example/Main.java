package org.example;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {

        if (args.length >= 1 && ("org.example.Main".equals(args[0]) || "Main".equals(args[0]))) {
            args = Arrays.copyOfRange(args, 1, args.length);
            System.err.println("DEBUG ARGS (trimmed): " + Arrays.toString(args));
        }

        if (args.length != 2) {
            System.err.println("Usage: org.example.Main <input path> <output path>");
            System.exit(2);
        }
        String in  = args[0];
        String out = args[1];

        // no need for -D flags
        Configuration conf = new Configuration();
        conf.setInt("col.voted_up", 4);
        conf.setInt("col.language", 1);
        conf.setInt("col.early", 11);
        conf.set("filter.language", "english");

        Job job = Job.getInstance(conf, "Steam Reviews");
        job.setJarByClass(Main.class);
        job.setMapperClass(ReviewMapper.class);
        job.setCombinerClass(ReviewCombiner.class);
        job.setReducerClass(ReviewReducer.class);

        // combine csv so less mappers
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path(in));
        CombineTextInputFormat.setMaxInputSplitSize(job, 128L * 1024 * 1024); // 128MB

        // Map outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritable.class);
        // Final outputs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(out));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
