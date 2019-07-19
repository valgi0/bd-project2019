package it.unibo.bd1819.job2.reducers;

import it.unibo.bd1819.job2.utils.Utility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CounterRatingReducer extends Reducer<Text, IntWritable, Text, Text> {

    private Logger logger = Logger.getLogger(CounterRatingReducer.class);

    /**
     * Reduce tasks that compute the average rating of each book and the number of bookmarks
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Double rating = 0.0;

        Integer countBM = 0;
        int countRating = 0;
        for (IntWritable r : values) {
            // it checks if value is BOOKMARK token
            if (r.equals(new IntWritable(Utility.IDENTIFIER))) {
                countBM++;
            } else {
                // or it is a rating
                rating += Double.parseDouble(r.toString());
                countRating++;
            }
        }
        Double avg = 0.0;
        // now compute the avg rating for the book
        if (countRating > 0) avg = rating / countRating;

        // and now using bookmarks and rating chooses the group to put the book
        String group = "";
        if(avg < Utility.RATING_THRESHOLD && countBM < Utility.BM_THRESHOLD){
            group = Utility.LOWBMLOWRATING;
        }
        if(avg >= Utility.RATING_THRESHOLD && countBM < Utility.BM_THRESHOLD){
            group = Utility.LOWBMHIGHRATING;
        }
        if(avg >= Utility.RATING_THRESHOLD && countBM >= Utility.BM_THRESHOLD){
            group = Utility.HIGHBMHIGHRATING;
        }
        if(avg < Utility.RATING_THRESHOLD && countBM >= Utility.BM_THRESHOLD){
            group = Utility.HIGHBMLOWRATING;
        }

        // it saves just the group
        context.write( new Text(group), new Text(""));
    }
}
