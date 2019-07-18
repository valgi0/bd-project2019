package it.unibo.bd1819.job2.mappers;

import it.unibo.bd1819.job2.utils.FileParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class CounterRatingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * This calss does a map from the line read in reating.csv and a pair
     * id_book as key and rating as value
     * @param key offset
     * @param value csv line
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        List<String> values = FileParser.parseCSVLine(value.toString());
        String idBook = values.get(1);
            if (!values.get(2).equals("rating")) {
                IntWritable rating = new IntWritable(Integer.parseInt(values.get(2)));
                context.write(new Text(idBook), rating);
            }
    }
}
