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

   /* public static Job countBookmarksJob() throws IOException{

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

    public static Job countRatingJob () throws IOException{
        return Job.getInstance(conf);
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }

}
