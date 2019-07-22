package it.unibo.bd1819.job1.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Questo Reducer si occupa di riportare il punteggio al valore che aveva prima della modifica subita nel reducer del
 * job filter
 */

public class SortReduce extends Reducer<Text, Text, Text, Text> {
    /**
     * @param key     media del punteggio dei libri scritti dall'autore
     * @param value   autore
     * @param context di hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        Iterator<Text> values = value.iterator();
        while (values.hasNext()) {
            String autore = values.next().toString();
            Float punteggio = Float.parseFloat(key.toString()) * (-1) + 5;
            context.write(new Text(autore), new Text(punteggio.toString()));

        }
    }
}
