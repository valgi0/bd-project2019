package it.unibo.bd1819.job2.mappers;

import it.unibo.bd1819.job2.utils.FileParser;
import it.unibo.bd1819.job2.utils.Utility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * This mapper is used to create the list of each books in each groups
     *
     * It accepts a TextInputFormat
     *
     * @param key Offset number
     * @param value CSV line
     * @param context Context of hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        Text newKey = new Text(value);
        IntWritable newValue =new IntWritable(Utility.IDENTIFIER);
        context.write(newKey, newValue);
    }
}
