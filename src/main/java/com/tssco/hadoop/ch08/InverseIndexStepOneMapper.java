package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class InverseIndexStepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (value != null) {
            String line = value.toString();
            String[] words = line.split(" ");
            if (words != null && words.length > 0) {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String fileName = fileSplit.getPath().getName();
                for (String word : words) {
                    context.write(new Text(word + " --> " + fileName), new LongWritable(1));
                }
            }
        }
    }
}
