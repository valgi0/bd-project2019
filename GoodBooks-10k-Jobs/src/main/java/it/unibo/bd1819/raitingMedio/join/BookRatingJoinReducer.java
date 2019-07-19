package it.unibo.bd1819.raitingMedio.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BookRatingJoinReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Float count = (float) 0;
		Float sum = (float) 0;
		Iterator<Text> values = value.iterator();
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
			if (attore.startsWith(" "))attore=attore.replaceFirst(" ","");
			context.write(new Text(attore), new Text((new Float(sum/count)).toString()));
		}
	}
}
