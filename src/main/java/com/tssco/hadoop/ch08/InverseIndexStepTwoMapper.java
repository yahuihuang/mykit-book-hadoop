package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class InverseIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        if (value != null) {
            String line = value.toString();
            String[] fileds = line.split("\t");
            String[] wordAndFileName = fileds[0].split(" --> ");
            String word = wordAndFileName[0];
            String fileName = wordAndFileName[1];
            long count = Long.parseLong(fileds[1]);
            context.write(new Text(word), new Text(fileName + " --> " + count));
        }

    }
}
