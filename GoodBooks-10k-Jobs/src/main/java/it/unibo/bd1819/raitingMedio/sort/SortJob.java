package it.unibo.bd1819.raitingMedio.sort;


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

public class SortJob {


    public static Job istance(final Configuration conf) throws Exception {
        Path inputPath3 = new Path("outputFilter"),
                outputPath2 = new Path("outputSort"),
                partition= new Path ("partition");
        FileSystem fs2 = FileSystem.get(new Configuration());

        if (fs2.exists(outputPath2)) {
            fs2.delete(outputPath2, true);
        }
        if (fs2.exists(partition)) fs2.delete(partition,true);
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Sort medio-Libro");
        joinPrincipalBasicJob.setReducerClass(SortReduce.class);
        joinPrincipalBasicJob.setMapperClass(SortMapper.class);

        joinPrincipalBasicJob.setJarByClass(Main.class);


        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(Text.class);

        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);
        joinPrincipalBasicJob.setNumReduceTasks(3);
        joinPrincipalBasicJob.setInputFormatClass(KeyValueTextInputFormat.class);
        joinPrincipalBasicJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(joinPrincipalBasicJob, inputPath3);
        joinPrincipalBasicJob.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(joinPrincipalBasicJob.getConfiguration(), new Path("partition", "part.lst"));
        InputSampler.writePartitionFile(joinPrincipalBasicJob, new InputSampler.RandomSampler<>(1, 1000));
        FileOutputFormat.setOutputPath(joinPrincipalBasicJob, outputPath2);

        return joinPrincipalBasicJob;
    }
}
