package it.unibo.bd1819.job1;

import it.unibo.bd1819.job1.filter.FilterJob;
import it.unibo.bd1819.job1.join.JoinJob;
import it.unibo.bd1819.job1.sort.SortJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {

        List<Job> jobs = new ArrayList<>();

        Configuration conf = new Configuration();
        jobs.add(JoinJob.istance(conf));
        jobs.add(FilterJob.istance(conf));
        jobs.add(SortJob.istance(conf));

        for (Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }
}

