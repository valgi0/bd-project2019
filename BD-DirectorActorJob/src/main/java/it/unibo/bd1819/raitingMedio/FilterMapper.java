package it.unibo.bd1819.raitingMedio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;


public class FilterMapper   extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value,
                    OutputCollector<Text,FloatWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        String[] tmp = line.split("\t");
        String autore=tmp[0];
        String score=tmp[1];
        output.collect(new Text(autore),new FloatWritable(Float.parseFloat(score)));
    }
}
