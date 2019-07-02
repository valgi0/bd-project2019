package it.unibo.bd1819.raitingMedio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class MyPartitioner implements Partitioner<FloatWritable, Text> {
    public int getPartition(FloatWritable key, Text value, int reduceTask) {
        if (key.get()<3.739324) return 0;
        else if (key.get()<3.863897) return 1;
        else if (key.get()<3.979264) return 2;
        else if (key.get()<4.09323) return 3;
        else return 4;
    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
