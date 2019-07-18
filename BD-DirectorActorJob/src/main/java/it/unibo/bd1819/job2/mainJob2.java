package it.unibo.bd1819.job2;

import it.unibo.bd1819.job2.utils.JobsFactory;

import java.io.IOException;

public class mainJob2 {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        //JobsFactory.countBookmarksJob().waitForCompletion(true);
        JobsFactory.computeAVGRatingJob().waitForCompletion(true);
    }
}
