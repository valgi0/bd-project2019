package it.unibo.bd1819.raitingMedio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import it.unibo.bd1819.raitingMedio.filter.*;
import it.unibo.bd1819.raitingMedio.join.*;
import it.unibo.bd1819.raitingMedio.sort.*;


import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length ==0) return;

        List<Job> jobs = new ArrayList<>();

        Configuration conf = new Configuration();
        if (args[0].equals("100")) jobs.add(JoinJob.istance(conf));
        if (args[0].equals("010"))jobs.add(FilterJob.istance(conf));
        if (args[0].equals("001"))jobs.add(SortJob.istance(conf));


        for (Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }
}

