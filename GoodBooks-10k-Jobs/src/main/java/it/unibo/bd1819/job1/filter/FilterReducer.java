package it.unibo.bd1819.job1.filter;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Questo mapper calcola la media dei rating per ogni autore, il rating va da 1 a 5.
 * Volendo il risultato ordinato in maniera decrescente trasformo il valore del rating in modo che i numeri più alti diventino
 * più bassi
 */

public class FilterReducer extends Reducer<Text, FloatWritable, Text, Text> {
    /**
     * @param key     autore
     * @param value   punteggio dei libri scritti dall'autore
     * @param context di hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> values = value.iterator();
        int count = 0;
        float sum = (float) 0;
        while (values.hasNext()) {
            count++;
            sum += values.next().get();
        }
        context.write(new Text(((new Float(((sum / count) - 5) * (-1)))).toString()), new Text(key));
    }
}
