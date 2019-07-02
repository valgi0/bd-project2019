package it.unibo.bd1819.raitingMedio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SortMapper  extends MapReduceBase
        implements Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value,
                    OutputCollector<Text,Text> output, Reporter reporter)
            throws IOException {
        String autore=key.toString();
        String score=value.toString();
        output.collect(new Text(String.valueOf(new Float((Float.parseFloat(score)-5)*(-1)))), new Text(autore));

    }
}
