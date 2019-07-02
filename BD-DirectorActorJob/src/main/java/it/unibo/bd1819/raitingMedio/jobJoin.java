package it.unibo.bd1819.raitingMedio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class jobJoin {
	public static void main(String[] args) throws IOException {

		/*
	}
		JobConf jobJoin = new JobConf(raitingMedio.jobJoin.class);
		jobJoin.setJobName("Join Raiting medio-Libro");
		Path inputPath1 = new Path("ratings.csv"),
				inputPath2=new Path("books.csv"),
				outputPath = new Path("outputJoin");
		FileSystem fs = FileSystem.get(new Configuration());

		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		MultipleInputs.addInputPath(jobJoin, inputPath1,
				TextInputFormat.class, RaitingMapper.class);
		MultipleInputs.addInputPath(jobJoin, inputPath2,
				TextInputFormat.class, BookMapper.class);
		FileOutputFormat.setOutputPath(jobJoin, outputPath);
		jobJoin.setReducerClass(BookRatingJoinReducer.class);
		jobJoin.setNumReduceTasks(3);
		jobJoin.setMapOutputKeyClass(Text.class);
		jobJoin.setMapOutputValueClass(Text.class);
		jobJoin.setOutputKeyClass(Text.class);
		jobJoin.setOutputValueClass(Text.class);
		JobClient.runJob(jobJoin);
/////////////////												JOB2
*/

		JobConf jobFilter = new JobConf(jobJoin.class);
		jobFilter.setJobName("Sorting Raiting medio-Libro");
		Path inputPath4 = new Path("outputJoin"),
				outputPath3 = new Path("outputFilter");
		FileSystem fs3 = FileSystem.get(new Configuration());

		if(fs3.exists(outputPath3)) {
			fs3.delete(outputPath3, true);
		}
		FileInputFormat.addInputPath(jobFilter, inputPath4);
		TextOutputFormat.setOutputPath(jobFilter, outputPath3);
		jobFilter.setMapperClass(FilterMapper.class);
		jobFilter.setReducerClass(FilterReducer.class);
		jobFilter.setNumReduceTasks(4);
		jobFilter.setMapOutputKeyClass(Text.class);
		jobFilter.setMapOutputValueClass(FloatWritable.class);
		jobFilter.setOutputKeyClass(Text.class);
		jobFilter.setOutputValueClass(Text.class);
		JobClient.runJob(jobFilter);


		///////////////											JOB3


		// Create job and parse CLI parameters








		JobConf jobSort = new JobConf(jobJoin.class);


		jobSort.setJobName("Sorting Raiting medio-Libro");
		Path inputPath3 = new Path("outputFilter"),
				outputPath2 = new Path("sortRatingBook.csv");
		FileSystem fs2 = FileSystem.get(new Configuration());

		if(fs2.exists(outputPath2)) {
			fs2.delete(outputPath2, true);
		}
		KeyValueTextInputFormat.addInputPath(jobSort, inputPath3);
		TextOutputFormat.setOutputPath(jobSort, outputPath2);
		jobSort.setMapperClass(SortMapper.class);
		jobSort.setReducerClass(SortReduce.class);
		jobSort.setNumReduceTasks(5);
		jobSort.setMapOutputKeyClass(Text.class);
		jobSort.setMapOutputValueClass(Text.class);
		jobSort.setOutputKeyClass(Text.class);
		jobSort.setOutputValueClass(Text.class);

		TotalOrderPartitioner.setPartitionFile(jobSort, new Path( "hdfs:///user/rsoro/partition", "part.lst"));
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(1, 1000);
		try {
			InputSampler.writePartitionFile(Job.getInstance(jobSort), sampler);
		} catch (Exception e) {
			e.printStackTrace();
		}
		JobClient.runJob(jobSort);
	}
}
