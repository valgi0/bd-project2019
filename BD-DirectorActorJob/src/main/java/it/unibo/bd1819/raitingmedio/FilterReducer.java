package it.unibo.bd1819.raitingmedio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class FilterReducer   extends MapReduceBase implements Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        int count=0;
        float sum= (float) 0;
        while (values.hasNext()) {
            count++;
            sum+= values.next().get();
        }
        output.collect(new Text(key), new Text((new Float(sum/count)).toString()));
    }
}
