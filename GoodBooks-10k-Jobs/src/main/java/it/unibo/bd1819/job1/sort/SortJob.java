package it.unibo.bd1819.job1.sort;


import it.unibo.bd1819.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * questo job si occupa di ordinare i risultati del job precedente attraverso il TotalOrderPartitioner
 */
public class SortJob {


    public static Job istance(final Configuration conf) throws Exception {
        Path inputPath = new Path("./exam/output/outputFilter"),
                outputPath = new Path("./exam/output/outputSort"),
                partition = new Path("./exam/partition");
        FileSystem fs2 = FileSystem.get(new Configuration());

        if (fs2.exists(outputPath)) {
            fs2.delete(outputPath, true);
        }
        if (fs2.exists(partition)) fs2.delete(partition, true);
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Sort medio-Libro");
        joinPrincipalBasicJob.setReducerClass(SortReduce.class);
        joinPrincipalBasicJob.setMapperClass(SortMapper.class);

        joinPrincipalBasicJob.setJarByClass(Main.class);

        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(Text.class);
        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);
        joinPrincipalBasicJob.setInputFormatClass(KeyValueTextInputFormat.class);
        joinPrincipalBasicJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(joinPrincipalBasicJob, inputPath);
        joinPrincipalBasicJob.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(joinPrincipalBasicJob.getConfiguration(), partition);
        InputSampler.writePartitionFile(joinPrincipalBasicJob, new InputSampler.RandomSampler<>(1, 1000));
        FileOutputFormat.setOutputPath(joinPrincipalBasicJob, outputPath);

        return joinPrincipalBasicJob;
    }
}
