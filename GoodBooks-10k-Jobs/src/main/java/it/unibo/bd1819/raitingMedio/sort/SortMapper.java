package it.unibo.bd1819.raitingMedio.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value,
                    Context context)
            throws IOException, InterruptedException {
        String autore = value.toString();
        String score = key.toString();
        //context.write(new Text(String.valueOf(new Float((Float.parseFloat(score) - 5) * (-1)))), new Text(autore));
        context.write(new Text(score), new Text(autore));
    }
}
