package org.example;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, PairWritable> {
    public enum COUNTERS {HEADER, CORRUPT, RECORDS, FILTERED}

    private final Text output = new Text();
    private final PairWritable outPair = new PairWritable();

    private int votedUpIdx, langIdx, earlyIdx;
    private String langKeep;

    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        votedUpIdx = conf.getInt("col.voted_up", 4);
        langIdx    = conf.getInt("col.language", 1);
        earlyIdx   = conf.getInt("col.early", 11);
        langKeep   = conf.get("filter.language", "english").toLowerCase();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.isEmpty()) return;

        FileSplit split = (FileSplit) context.getInputSplit();
        if (split.getStart() == 0) {
            String lower = line.toLowerCase();

            if (lower.contains("language") || lower.contains("voted") || lower.contains("early")) {
                context.getCounter(COUNTERS.HEADER).increment(1);
                return;
            }
        }

        try {
            String fileName = split.getPath().getName();
            String base = fileName;
            if (base.endsWith(".gz"))  base = base.substring(0, base.length() - 3);
            if (base.endsWith(".csv")) base = base.substring(0, base.length() - 4);
            String appid = base;

            List<String> columns = CsvParser.parse(line);
            int n = columns.size();
            if (votedUpIdx >= n || langIdx >= n || earlyIdx >= n) {
                context.getCounter(COUNTERS.CORRUPT).increment(1);
                return;
            }

            String language = columns.get(langIdx).trim().toLowerCase();
            String isEarlyAccess = columns.get(earlyIdx).trim().toLowerCase();

            boolean early = isEarlyAccess.equals("true") || isEarlyAccess.equals("1") || isEarlyAccess.equals("yes");
            if (!language.equals(langKeep) || early) {
                context.getCounter(COUNTERS.FILTERED).increment(1);
                return;
            }

            String upVoted = columns.get(votedUpIdx).trim().toLowerCase();
            boolean isUpVoted = upVoted.equals("true") || upVoted.equals("1") || upVoted.equals("yes");

            output.set(appid);
            outPair.setPositive(isUpVoted ? 1 : 0, 1);
            context.write(output, outPair);
            context.getCounter(COUNTERS.RECORDS).increment(1);
        } catch (Exception e){
            context.getCounter(COUNTERS.CORRUPT).increment(1);
        }
    }
}
