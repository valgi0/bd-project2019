package it.unibo.bd1819.raitingMedio.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RaitingMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
					Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tmp = line.split(",");
		String book=tmp[1];
		String raiting=tmp[2];
		if (!raiting.equals("rating")) {
			context.write(new Text(book), new Text("2"+raiting));
		}
	}
}
