package edu.usfca.cs.mr.climate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created By: Melanie Baybay
 * Last Modified: 11/13/17
 */
public class ClimateJob {
    public static void main(String[] args) {
        if(args.length != 3) {
            System.err.println("Usage:\n yarn jar ./target/project2-1.0.jar edu.usfca.cs.mr.climate.ClimateJob <geo> <input-file(s)> <output-file>");
            System.exit(-1);
        }

        try {
            Configuration conf = new Configuration();
            conf.set("geo", args[0]);
            Job job = Job.getInstance(conf, "climate");
            job.setJarByClass(ClimateJob.class);
            // Mapper
            job.setMapperClass(ClimateMapper.class);
            // Reducer
            job.setReducerClass(ClimateReducer.class);
            // set mapper key and value class
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // set reducer key and value class
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

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
