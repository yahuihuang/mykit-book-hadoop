package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class DataJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String filePathInHDFS = ((FileSplit) context.getInputSplit()).getPath().toString();
        String dataFileTag = null;
        String dataJoinKey = null;
        String dataJoinValue = null;

        String line = value.toString();
        String[] words = line.split(" ");
        if (filePathInHDFS.contains(DataJoinConstants.JOIN_LEFT_FILENAME)) {
            dataFileTag = DataJoinConstants.JOIN_LEFT_FILENAME_TAG;
            dataJoinKey = words[1];
            dataJoinValue = words[0];
        } else if (filePathInHDFS.contains(DataJoinConstants.JOIN_RIGHT_FILENAME)) {
            dataFileTag = DataJoinConstants.JOIN_RIGHT_FILENAME_TAG;
            dataJoinKey = words[0];
            dataJoinValue = words[1];
        }

        if (dataJoinValue != null) {
            dataJoinValue = dataJoinValue.concat(" ").concat(dataFileTag);
            context.write(new Text(dataJoinKey), new Text(dataJoinValue));
        }
    }
}
