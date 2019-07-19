package it.unibo.bd1819.raitingMedio.count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CountReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        Iterator<Text> values = value.iterator();
        while (values.hasNext()) {
            String attore = values.next().toString();
            Float punteggio = Float.parseFloat(key.toString())*(-1)+5;
            context.write(new Text(attore), new Text(punteggio.toString()));

        }
    }
}
