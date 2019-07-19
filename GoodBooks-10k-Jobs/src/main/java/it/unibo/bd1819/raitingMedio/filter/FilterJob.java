package it.unibo.bd1819.raitingMedio.filter;


import it.unibo.bd1819.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FilterJob {



    public static Job istance(final Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path inputPath4 = new Path("outputJoin"),
                outputPath3 = new Path("outputFilter");
        if(fs.exists(outputPath3)) {
            fs.delete(outputPath3, true);
        }
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Filtering Raiting medio-Libro");
        joinPrincipalBasicJob.setReducerClass(FilterReducer.class);
        joinPrincipalBasicJob.setMapperClass(FilterMapper.class);
        joinPrincipalBasicJob.setJarByClass(Main.class);

        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(FloatWritable.class);

        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(joinPrincipalBasicJob, outputPath3);
        TextInputFormat.addInputPath(joinPrincipalBasicJob, inputPath4);
        joinPrincipalBasicJob.waitForCompletion(true);
        return joinPrincipalBasicJob;
    }
}
