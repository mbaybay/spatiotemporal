package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MAPPER: If lightning_surface > 0, then output:
 *      (key) geohash-prefix w/ 4 precision (value) 1
 * Created By: Melanie Baybay
 * Last Modified: 10/31/17
 */
public class LightningMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t");
        double lightning_surface = Double.parseDouble(record[22]);
        if (lightning_surface > 0) {
            String geohash = record[1].substring(0, 4);
            context.write(new Text(geohash), new IntWritable(1));
        }
    }
}
