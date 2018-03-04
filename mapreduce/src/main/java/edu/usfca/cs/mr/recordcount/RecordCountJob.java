package edu.usfca.cs.mr.recordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created By: Melanie Baybay
 * Last Modified: 10/28/17
 */
public class RecordCountJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "record count job");
            job.setJarByClass(RecordCountJob.class);
            // Mapper
            job.setMapperClass(RecordCountMapper.class);
            // Combiner
            job.setCombinerClass(RecordCountReducer.class);
            // Reducer
            job.setReducerClass(RecordCountReducer.class);
            // set mapper key and value class
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // set reducer key and value class
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

//            FileInputFormat.setInputDirRecursive(job, true);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
    /** ------------------------------------------------------------------------------------- **/
}
