package com.tssco.hadoop.ch08;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final Logger logger = LoggerFactory.getLogger(WordCountCombiner.class);

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        logger.info("Combiner階段輸分組: (" + key.toString() + ")");
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
            logger.info("Combiner階段輸鍵值對: (" + key.toString() + ", " + value.get() + ")");
        }

        logger.info("Combiner階段輸鍵值對: (" + key.toString() + ", " + sum + ")");
        context.write(key, new LongWritable(sum));
    }
}
