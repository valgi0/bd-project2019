package raitingMedio;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RaitingMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value,
					OutputCollector<Text,Text> output, Reporter reporter)
					throws IOException {
		String line = value.toString();
		String[] tmp = line.split(",");
		String book=tmp[1];
		String raiting=tmp[2];
			if (raiting.equals("rating")) return;
			else output.collect(new Text(book), new Text("2"+raiting));

	}
}
