package it.unibo.bd1819.job2.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CounterBMJobReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Reducer for count how many bookmarks are listed for a book
     * @param key the book_id
     * @param values List of bookmarks
     * @param context
     */

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable mark : values){
            count ++;
        }

        context.write(key, new IntWritable(count));
    }

}
