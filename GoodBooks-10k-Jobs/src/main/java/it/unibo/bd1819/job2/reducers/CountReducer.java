package it.unibo.bd1819.job2.reducers;

import it.unibo.bd1819.job2.utils.Utility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Reducer for count how many book are listed for each groups
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
