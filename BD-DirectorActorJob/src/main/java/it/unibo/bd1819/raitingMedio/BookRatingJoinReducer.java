package raitingMedio;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BookRatingJoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
					   Reporter reporter) throws IOException {
		Float count = (float) 0;
		Float sum = (float) 0;
		List<String> attori = new ArrayList<>();
		while (values.hasNext()) {
			String valore = values.next().toString();
			if (valore.substring(0, 1).equals("1")) {
				valore = valore.substring(1);
				if (valore.contains(";")) attori = Arrays.asList(valore.split(";"));
				else attori.add(valore);
			} else {
				valore = valore.substring(1);
				count++;
				sum = sum + Integer.parseInt(valore);
			}
		}
		for (String attore : attori){
			output.collect(new Text(attore), new Text((new Float(sum/count)).toString()));
		}
	}
}
