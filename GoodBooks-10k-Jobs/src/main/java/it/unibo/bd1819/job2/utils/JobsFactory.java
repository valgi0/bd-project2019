package it.unibo.bd1819.job2.utils;

import it.unibo.bd1819.job2.mainJob2;
import it.unibo.bd1819.job2.mappers.CountMapper;
import it.unibo.bd1819.job2.mappers.CounterBMJobMapper;
import it.unibo.bd1819.job2.mappers.CounterRatingMapper;
import it.unibo.bd1819.job2.reducers.CountReducer;
import it.unibo.bd1819.job2.reducers.CounterBMJobReducer;
import it.unibo.bd1819.job2.reducers.CounterRatingReducer;
import it.unibo.bd1819.raitingMedio.join.BookMapper;
import it.unibo.bd1819.raitingMedio.join.RaitingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobsFactory {

    private enum pathToFiles{
        RATING ("./bd-project2019-master/dataset/ratings.csv"),
        BOOKMARKS ("./bd-project2019-master/dataset/to_read.csv"),
        OUTJOB2 ("./bd-project2019-master/dataset/output/it.unibo.bd1819.job2"),
        OUTJOB3 ("./bd-project2019-master/dataset/output/job3");

        String path;
        pathToFiles(String path){
            this.path = path;
        }

        Path getPath(){
            return new Path(this.path);
        }
    }

    private static final Configuration conf = new Configuration();


    /**
     * This job is composed by two mapper and one reducer.
     *  <li> Mapper 1: create a couple Key value where key is book_id and value the rating</li>
     *  <li> Mapper 2: Create a couple Key Value where key is book_id and value a token rappresenting the Bookmarks</li>
     *  <li> Reducer get the couple where key are book_id and value a list of element and check each element
     *  if it is a number between rating range  Reducer use it to comput average rating. Otherwise, if it
     *  is a token it uses it to compute how many bookmarks the book has</li>
     *
     * At the end of this job we get a list of groups.
     *
     *  Groups: The group are four fields where book is placed according to the number of bookmarks and rating:
     *  <li>High Rating High Bookmarks</li>
     *  <li>Low Rating Low Bookmarks </li>
     *  <li>Low Rating High Bookmarks</li>
     *  <li>High Rating Low Bookmarks</li>
     * @return
     * @throws IOException
     */
    public static Job computeAVGRatingJob() throws IOException{
        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, pathToFiles.OUTJOB2.getPath());

        // create jobs
        Job avgratingjobs = Job.getInstance(conf, "AVG Rating counter for each books");
        avgratingjobs.setJarByClass(mainJob2.class);

        //setting mapper and reducer
        avgratingjobs.setReducerClass(CounterRatingReducer.class);

        //Setting outputs
        avgratingjobs.setMapOutputKeyClass(Text.class);
        avgratingjobs.setMapOutputValueClass(IntWritable.class);
        avgratingjobs.setOutputKeyClass(Text.class);
        avgratingjobs.setOutputValueClass(Text.class);


        avgratingjobs.setOutputFormatClass(TextOutputFormat.class);
        // setting input output files
        FileOutputFormat.setOutputPath(avgratingjobs, pathToFiles.OUTJOB2.getPath());
        MultipleInputs.addInputPath(avgratingjobs, pathToFiles.RATING.getPath(),
                TextInputFormat.class, CounterRatingMapper.class);
        MultipleInputs.addInputPath(avgratingjobs, pathToFiles.BOOKMARKS.getPath(),
                TextInputFormat.class, CounterBMJobMapper.class);

        return avgratingjobs;

    }


    /**
     * This jobs just read the output of the computeAVGRatingJob() and count how many books are in each fields
     * At the end we get 4 tuples with Group_type, number_of_books
     * @return
     * @throws IOException
     */
    public static Job counterJob() throws IOException{
        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, pathToFiles.OUTJOB3.getPath());

        // create jobs
        Job job = Job.getInstance(conf, "classificator counter");
        job.setJarByClass(mainJob2.class);

        //setting mapper and reducer
        job.setReducerClass(CountReducer.class);
        job.setMapperClass(CountMapper.class);

        //Setting outputs
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        // setting input output files
        FileOutputFormat.setOutputPath(job, pathToFiles.OUTJOB3.getPath());
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,pathToFiles.OUTJOB2.getPath());

        return job;

    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }

}
