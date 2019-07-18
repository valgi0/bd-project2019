package it.unibo.bd1819.raitingMedio.join;


import it.unibo.bd1819.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinJob {
    public static Job istance(final Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path inputPath1 = new Path("ratings.csv"),
                inputPath2 = new Path("books.csv"),
                outputPath = new Path("outputJoin");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Join Raiting medio-Libro");
        joinPrincipalBasicJob.setReducerClass(BookRatingJoinReducer.class);
        joinPrincipalBasicJob.setJarByClass(Main.class);

        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(Text.class);

        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(joinPrincipalBasicJob, outputPath);
        MultipleInputs.addInputPath(joinPrincipalBasicJob, inputPath1,
                TextInputFormat.class, RaitingMapper.class);
        joinPrincipalBasicJob.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(joinPrincipalBasicJob, inputPath2,
                TextInputFormat.class, BookMapper.class);
        joinPrincipalBasicJob.waitForCompletion(true);
        return joinPrincipalBasicJob;
    }
}
