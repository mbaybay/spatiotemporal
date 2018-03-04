package edu.usfca.cs.mr.greenenergy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MAPPER: pass record onto reducer
 *
 * Created By: Melanie Baybay
 * Last Modified: 11/13/17
 */
public class FilterEnergyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t", 2);
        context.write(new Text(record[0]), new Text(record[1]));
    }
}
