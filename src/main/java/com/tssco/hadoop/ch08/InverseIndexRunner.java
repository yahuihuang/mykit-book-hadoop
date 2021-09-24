package com.tssco.hadoop.ch08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class InverseIndexRunner extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new InverseIndexRunner(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (runStepOneMapReduce(args) == false)
            return 1;
        return runStepTwoMapReduce(args) ? 0 : 1;
    }

    private static boolean runStepOneMapReduce(String[] args)
        throws Exception {
        Job job = getJob();
        job.setJarByClass(InverseIndexRunner.class);
        job.setMapperClass(InverseIndexStepOneMapper.class);
        job.setReducerClass(InverseIndexStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true);
    }

    public static boolean runStepTwoMapReduce(String[] args)
        throws Exception {
        Job job = getJob();
        job.setJarByClass(InverseIndexRunner.class);
        job.setMapperClass(InverseIndexStepTwoMapper.class);
        job.setReducerClass(InverseIndexStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true);
    }

    private static Job getJob() throws IOException {
        Configuration conf = new Configuration();
        return Job.getInstance(conf);
    }
}
