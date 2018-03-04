package edu.usfca.cs.mr.superhot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created By: Melanie Baybay
 * Last Modified: 10/29/17
 */
public class HottestJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "hottest job");
            job.setJarByClass(HottestJob.class);
            // Mapper
            job.setMapperClass(HottestMapper.class);
            // Combiner
            job.setCombinerClass(HottestCombiner.class);
            // Reducer
            job.setReducerClass(HottestReducer.class);
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
