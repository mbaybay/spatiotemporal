package edu.usfca.cs.mr.greenenergy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created By: Melanie Baybay
 * Last Modified: 11/12/17
 */
public class GreenEnergyJob {
    public static void main(String[] args) {
        try {
            String inPath = args[0];
            String outPath = args[1];
            String tempPath = args[1] + "-temp";
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "green_energy");
            job1.setJarByClass(GreenEnergyJob.class);
            // Mapper
            job1.setMapperClass(GreenEnergyMapper.class);
            // Reducer
            job1.setReducerClass(GreenEnergyReducer.class);
            // set mapper key and value class
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            // set reducer key and value class
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(inPath));
            FileOutputFormat.setOutputPath(job1, new Path(tempPath));

            boolean job1complete = job1.waitForCompletion(true);

            if(job1complete) {
                Job job2 = Job.getInstance(conf, "filter_green_energy");
                // Mapper
                job2.setMapperClass(FilterEnergyMapper.class);
                // Reducer
                job2.setReducerClass(FilterEnergyReducer.class);
                // set mapper key and value class
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                // set reducer key and value class
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job2, new Path(tempPath));
                FileOutputFormat.setOutputPath(job2, new Path(outPath));

                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
