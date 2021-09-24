package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class InverseIndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        if (values != null) {
            String result = "";
            for (Text value : values) {
                result = result.concat(value.toString()).concat(" ");
            }
            context.write(key, new Text(result));
        }
    }
}
