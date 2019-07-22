package it.unibo.bd1819.job1.filter;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Questo mapper riceve in input la coppia autore-rating e la passa in output
 */
public class FilterMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    /**
     * @param key     linea del file
     * @param value   autore e punteggio di uno dei libri scritti dall'autore, separati da tab
     * @param context Context di hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tmp = line.split("\t");
        String autore = tmp[0];
        String score = tmp[1];
        context.write(new Text(autore), new FloatWritable(Float.parseFloat(score)));
    }
}
