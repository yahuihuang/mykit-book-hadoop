package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortData, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String line = value.toString();
            String[] numbers = line.split(" ");
            final SortData k = new SortData(Long.parseLong(numbers[0]), Long.parseLong(numbers[1]));
            final LongWritable v = new LongWritable(Long.parseLong(numbers[1]));
            context.write(k, v);
        }
    }
}
