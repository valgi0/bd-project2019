package it.unibo.bd1819.raitingMedio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SortReduce  extends MapReduceBase implements Reducer<FloatWritable, Text, Text, Text> {

    public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        while (values.hasNext()) {
            String attore = values.next().toString();
            output.collect(new Text(attore), new Text((new Float((key.get()-5)*(-1))).toString()));

        }
    }
}
