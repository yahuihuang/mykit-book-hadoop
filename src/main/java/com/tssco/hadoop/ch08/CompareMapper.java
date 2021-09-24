package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CompareMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (value != null) {
            String line = value.toString();
            String[] numbers = line.split(" ");
            if (numbers != null && numbers.length > 0) {
                for (int iIdx = 0; iIdx < numbers.length; iIdx++) {
                    long temp = Long.parseLong(numbers[iIdx]);
                    if (temp > max) {
                        max = temp;
                    }
                    if (temp < min) {
                        min = temp;
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
        context.write(new LongWritable(max), new LongWritable(min));
    }
}
