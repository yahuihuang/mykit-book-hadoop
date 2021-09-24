package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountPartitioner extends HashPartitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
        return Math.abs(key.hashCode()) % numReduceTasks;
    }
}
