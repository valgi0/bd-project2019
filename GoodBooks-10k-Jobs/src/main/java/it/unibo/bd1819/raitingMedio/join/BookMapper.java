package it.unibo.bd1819.raitingMedio.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * legge e parsa il file book.csv, applica anche una fase di filtering escludendo i campi della tabella che non servono.
 * Dal mapper esce la coppia chiave-valore composta da ID libro e autori che lo hanno scritto
 */
public class BookMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * @param key     linea del file
     * @param value   linea del file csv
     * @param context di hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value,
                    Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        /**
         * Fase di parsing dei valori
         */
        List<String> tmp = Arrays.asList(line.split("\""));
        String newLine = "";
        for (String substring : tmp) {
            if (tmp.indexOf(substring) % 2 == 0) {
                newLine = newLine.concat(substring);
            } else {
                newLine = newLine.concat(substring.replaceAll(",", ";"));
            }
        }
        /**
         * Dine fase di parsing
         */
        String idLibro = newLine.split(",")[0];
        String autori = newLine.split(",")[7];
        context.write(new Text(idLibro), new Text("1" + autori));

    }
}
