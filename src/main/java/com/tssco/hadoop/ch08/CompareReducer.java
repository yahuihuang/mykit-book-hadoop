package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CompareReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        final long maxTemp = key.get();
        if (maxTemp > max) {
            max = maxTemp;
        }

        for (LongWritable value : values) {
            if (value.get() < min) {
                min = value.get();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(max), new LongWritable(min));
    }
}
