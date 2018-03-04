package edu.usfca.cs.mr.bayarea;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Identifies average precipitation (kg/m2) per month in the Bay Area.
 *
 * Created By: Melanie Baybay
 * Last Modified: 11/1/17
 */
public class BayAreaJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "bay area");
            job.setJarByClass(BayAreaJob.class);
            // Mapper
            job.setMapperClass(BayAreaMapper.class);
            // Reducer
            job.setReducerClass(BayAreaReducer.class);
            // set mapper key and value class
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // set reducer key and value class
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

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
}
