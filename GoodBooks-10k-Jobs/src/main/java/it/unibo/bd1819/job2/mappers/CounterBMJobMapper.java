package it.unibo.bd1819.job2.mappers;

import it.unibo.bd1819.job2.utils.FileParser;
import it.unibo.bd1819.job2.utils.Utility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class CounterBMJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * This function does a mapping. For each books in to_read table it creates a new Pair Key-value
     * composed by book_id as a key and 1 as value.
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
        List<String> values = FileParser.parseCSVLine(value.toString());
        Text newKey = new Text(values.get(1)); // book_id
        IntWritable newValue = new IntWritable(Utility.IDENTIFIER);
        context.write(newKey, newValue);
    }
}
