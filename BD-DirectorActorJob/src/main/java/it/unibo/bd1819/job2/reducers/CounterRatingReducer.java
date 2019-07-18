package it.unibo.bd1819.job2.reducers;

import it.unibo.bd1819.job2.utils.Utility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CounterRatingReducer extends Reducer<Text, Text, Text, Text> {

    private Logger logger = Logger.getLogger(CounterRatingReducer.class);

    /**
     * Reduce tasks that compute the average rating of each book
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Double rating = 0.0;

        int countBM = 0;
        int countRating = 0;
        for (IntWritable r : values) {
            if (r.equals(new IntWritable(Utility.IDENTIFIER))) {
                countBM++;
            } else {
                rating += Double.parseDouble(r.toString());
                countRating++;
            }
        }
        double avg = 0;
        if (countRating > 0) avg = rating / countRating;
        context.write(key, new Text(avg +"\t"+countBM));
    }
}
