package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<SortData, LongWritable, LongWritable, LongWritable> {
    @Override
    protected void reduce(SortData key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        context.write(new LongWritable(key.getFirst()), new LongWritable(key.getSecond()));
    }
}
