package it.unibo.bd1819.job2.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CounterRatingReducer extends Reducer<Text, Text, Text, Text> {

    private Logger logger = Logger.getLogger(CounterRatingReducer.class);

    /**
     * Reduce tasks that compute the average rating of each book
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double rating = 0.0;
        int count = 0;
        for(Text r : values){
            rating += Double.parseDouble(r.toString());
            count ++;
        }
        double avg = 0;
        if(count > 0)
            avg = rating / count;

        context.write(key, new Text(String.valueOf(avg)));
    }
}
