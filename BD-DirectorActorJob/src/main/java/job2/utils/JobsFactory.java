package job2.utils;

import job2.mappers.CounterBMJobMapper;
import job2.reducers.CounterBMJobReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class JobsFactory {

    private enum pathToFiles{
        BOOKS ("./bd-project2019-master/dataset/books.csv"),
        RATING ("./bd-project2019-master/dataset/rating.csv"),
        BOOKMARKS ("./bd-project2019-master/dataset/to_read.csv"),
        OUTJOB1 ("./bd-project2019-master/dataset/output/job1"),
        OUTJOB2 ("./bd-project2019-master/dataset/output/job2"),
        OUTJOB3 ("./bd-project2019-master/dataset/output/job3");

        String path;
        pathToFiles(String path){
            this.path = path;
        }

        String getPath(){
            return this.path;
        }
    }

    private static final Configuration conf = new Configuration();

    public static Job countBookmarksJob() throws IOException{

        FileSystem fs = FileSystem.get(conf);
        deleteOutputFolder(fs, new Path(pathToFiles.OUTJOB1.getPath()));

        // create jobs
        Job counterBMJobs = Job.getInstance(conf);

        //setting mapper and reducer
        counterBMJobs.setMapperClass(CounterBMJobMapper.class);
        counterBMJobs.setReducerClass(CounterBMJobReducer.class);

        

    }

    public static Job countRatingJob () throws IOException{
        return Job.getInstance(conf);
    }

    private static void deleteOutputFolder(final FileSystem fs, final Path folderToDelete) throws IOException {
        if (fs.exists(folderToDelete)) {
            fs.delete(folderToDelete, true);
        }
    }

}
