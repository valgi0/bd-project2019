package it.unibo.bd1819.raitingMedio.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * questo mapper manda in output i valori letti dal file di output del job FilterJob
 */

public class SortMapper extends Mapper<Text, Text, Text, Text> {
    /**
     * @param key     media dei punteggi di ogni libro scritto dall'autore
     * @param value   autore
     * @param context di hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Text key, Text value,
                    Context context)
            throws IOException, InterruptedException {
        String autore = value.toString();
        String score = key.toString();
        context.write(new Text(score), new Text(autore));
    }
}
