package it.unibo.bd1819.raitingMedio.count;


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
        Path inputPath = new Path("outputSort"),
                outputPath = new Path("outputCount");
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        Job joinPrincipalBasicJob = Job.getInstance(conf, "Count medio-Libro");
        joinPrincipalBasicJob.setReducerClass(CountReducer.class);
        joinPrincipalBasicJob.setMapperClass(CountMapper.class);

        joinPrincipalBasicJob.setJarByClass(Main.class);


        joinPrincipalBasicJob.setMapOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setMapOutputValueClass(Text.class);

        joinPrincipalBasicJob.setOutputKeyClass(Text.class);
        joinPrincipalBasicJob.setOutputValueClass(Text.class);
        joinPrincipalBasicJob.setNumReduceTasks(3);
        joinPrincipalBasicJob.setInputFormatClass(KeyValueTextInputFormat.class);
        joinPrincipalBasicJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(joinPrincipalBasicJob, inputPath);
        joinPrincipalBasicJob.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(joinPrincipalBasicJob.getConfiguration(), new Path("partition", "part.lst"));
        InputSampler.writePartitionFile(joinPrincipalBasicJob, new InputSampler.RandomSampler<>(1, 1000 ));
        FileOutputFormat.setOutputPath(joinPrincipalBasicJob, outputPath);

        return joinPrincipalBasicJob;
    }
}
