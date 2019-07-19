package it.unibo.bd1819.raitingMedio.filter;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FilterReducer extends Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> values = value.iterator();
        int count=0;
        float sum= (float) 0;
        while (values.hasNext()) {
            count++;
            sum+= values.next().get();
        }
        context.write( new Text(((new Float(((sum/count)-5)*(-1)))).toString()),new Text(key));
    }
}
