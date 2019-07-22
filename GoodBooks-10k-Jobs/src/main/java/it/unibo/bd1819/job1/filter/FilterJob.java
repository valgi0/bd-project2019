package it.unibo.bd1819.job1.filter;


import it.unibo.bd1819.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Questo job si occupa di filtrare le coppie autore-rating e di raggruppare tra di loro i rating di ogni autore per poi
 * farne la media.
 * Carica l'output del job JoinJob come input.
 */
public class FilterJob {


    public static Job istance(final Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path("./exam/output/outputJoin"),
                outputPath = new Path("./exam/output/outputFilter");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Filtering Raiting medio-Libro");
        joinPrincipalBasicJob.setReducerClass(FilterReducer.class);
        joinPrincipalBasicJob.setMapperClass(FilterMapper.class);
        joinPrincipalBasicJob.setJarByClass(Main.class);

        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(FloatWritable.class);

        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(joinPrincipalBasicJob, outputPath);
        TextInputFormat.addInputPath(joinPrincipalBasicJob, inputPath);
        joinPrincipalBasicJob.waitForCompletion(true);
        return joinPrincipalBasicJob;
    }
}
