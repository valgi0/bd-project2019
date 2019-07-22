package it.unibo.bd1819.job1.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Questo Reducer si occupa di gestire i dati provenienti dai due mapper BookMapper e RatingMapper e crea un elenco di coppie
 * chiave-valore con chiave l'autore e valore il punteggio
 */
public class BookRatingJoinReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * @param key     ID del libro
     * @param value   autori e punteggio del libro
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        Float count = (float) 0;
        Float sum = (float) 0;
        Iterator<Text> values = value.iterator();
        List<String> autori = new ArrayList<>();
        while (values.hasNext()) {
            String valore = values.next().toString();
            //se il valore inizia con 1 allora è una lista di autori separati da ;
            if (valore.substring(0, 1).equals("1")) {
                valore = valore.substring(1);
                if (valore.contains(";")) autori = Arrays.asList(valore.split(";"));
                else autori.add(valore);
            } else {
                //è il punteggio
                valore = valore.substring(1);
                count++;
                sum = sum + Integer.parseInt(valore);
            }
        }
        for (String u : autori) {
            if (u.startsWith(" ")) u = u.replaceFirst(" ", "");
            context.write(new Text(u), new Text((new Float(sum / count)).toString()));
        }
    }
}
