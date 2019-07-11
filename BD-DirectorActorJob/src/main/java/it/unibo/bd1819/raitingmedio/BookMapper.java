package it.unibo.bd1819.raitingmedio;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BookMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value,
					OutputCollector<Text,Text> output, Reporter reporter)
					throws IOException {
		String line = value.toString();
		List<String> tmp = Arrays.asList(line.split("\""));
		String newLine="";
		for(String substring : tmp){
			if (tmp.indexOf(substring)%2==0){
				//sono in uno split senza "
				newLine=newLine.concat(substring);
			}else{
				newLine=newLine.concat(substring.replaceAll(",",";"));
			}
		}
		String idLibro=newLine.split(",")[0];
		String autori=newLine.split(",")[7];
			output.collect(new Text(idLibro), new Text("1"+autori));

	}
}
