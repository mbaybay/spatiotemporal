package edu.usfca.cs.mr.travel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created By: Melanie Baybay
 * Last Modified: 11/5/17
 */
public class TravelJob {
    public static void main(String[] args) {
        if(args.length < 7) {
            System.err.println("Usage: <region1Geo> <region2Geo> <region3Geo> <region4Geo> <region5Geo> <input> <output>");
            System.exit(-1);
        }
        try {
            Configuration conf = new Configuration();

            conf.set("geo1", args[0]);
            conf.set("geo2", args[1]);
            conf.set("geo3", args[2]);
            conf.set("geo4", args[3]);
            conf.set("geo5", args[4]);

            Job job = Job.getInstance(conf, "travel");
            job.setJarByClass(TravelJob.class);

            // Mapper
            job.setMapperClass(TravelMapper.class);
            // Reducer
            job.setReducerClass(TravelReducer.class);
            // set mapper key and value class
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // set reducer key and value class
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[5]));
            FileOutputFormat.setOutputPath(job, new Path(args[6]));

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
