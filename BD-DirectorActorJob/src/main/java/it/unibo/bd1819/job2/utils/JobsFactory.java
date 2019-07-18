package it.unibo.bd1819.job2.utils;

import it.unibo.bd1819.job2.mainJob2;
import it.unibo.bd1819.job2.mappers.CounterBMJobMapper;
import it.unibo.bd1819.job2.mappers.CounterRatingMapper;
import it.unibo.bd1819.job2.reducers.CounterBMJobReducer;
import it.unibo.bd1819.job2.reducers.CounterRatingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class JobsFactory {

    private enum pathToFiles{
        BOOKS ("./bd-project2019-master/dataset/books.csv"),
        RATING ("./bd-project2019-master/dataset/ratings.csv"),
        BOOKMARKS ("./bd-project2019-master/dataset/to_read.csv"),
        OUTJOB1 ("./bd-project2019-master/dataset/output/job1"),
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

    public static Job countBookmarksJob() throws IOException{

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, pathToFiles.OUTJOB1.getPath());

        // create jobs
        Job counterBMJobs = Job.getInstance(conf, "BookMarks counter for each books");
        counterBMJobs.setJarByClass(mainJob2.class);

        //setting mapper and reducer
        counterBMJobs.setMapperClass(CounterBMJobMapper.class);
        counterBMJobs.setReducerClass(CounterBMJobReducer.class);

        //Setting outputs
        counterBMJobs.setMapOutputKeyClass(Text.class);
        counterBMJobs.setMapOutputValueClass(IntWritable.class);
        counterBMJobs.setOutputKeyClass(Text.class);
        counterBMJobs.setOutputValueClass(IntWritable.class);

        // setting input output files
        FileOutputFormat.setOutputPath(counterBMJobs, pathToFiles.OUTJOB1.getPath());
        FileInputFormat.addInputPath(counterBMJobs, pathToFiles.BOOKMARKS.getPath());

        return counterBMJobs;
    }

    public static Job computeAVGRatingJob() throws IOException{
        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, pathToFiles.OUTJOB2.getPath());

        // create jobs
        Job avgratingjobs = Job.getInstance(conf, "AVG Rating counter for each books");
        avgratingjobs.setJarByClass(mainJob2.class);

        //setting mapper and reducer
        avgratingjobs.setMapperClass(CounterRatingMapper.class);
        avgratingjobs.setReducerClass(CounterRatingReducer.class);

        //Setting outputs
        avgratingjobs.setMapOutputKeyClass(Text.class);
        avgratingjobs.setMapOutputValueClass(Text.class);
        avgratingjobs.setOutputKeyClass(Text.class);
        avgratingjobs.setOutputValueClass(IntWritable.class);

        // setting input output files
        FileOutputFormat.setOutputPath(avgratingjobs, pathToFiles.OUTJOB2.getPath());
        FileInputFormat.addInputPath(avgratingjobs, pathToFiles.RATING.getPath());

        return avgratingjobs;

    }

    public static Job countRatingJob () throws IOException{
        return Job.getInstance(conf);
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }

}