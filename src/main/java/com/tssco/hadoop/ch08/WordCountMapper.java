package com.tssco.hadoop.ch08;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value != null) {
            String line = value.toString();
            String[] words = StringUtils.split(line, " ");
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }
}
