package it.unibo.bd1819.job1.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * legge e parsa il file ratings.csv, applica anche una fase di filtering escludendo i campi della tabella che non servono.
 * Dal mapper esce la coppia chiave-valore composta da ID libro e rating
 */

public class RaitingMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * @param key     linea del file
     * @param value   linea del file csv
     * @param context di hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
        String line = value.toString();
        /**
         * Fase di parsing
         */
        String[] tmp = line.split(",");
        String book = tmp[1];
        String rating = tmp[2];
        if (!rating.equals("rating")) {
            //Escludo la prima
            context.write(new Text(book), new Text("2" + rating));
        }
    }
}
