package edu.usfca.cs.mr.superhot;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MAPPER: Output timestamp, geohash, and temperature_surface
 * Created By: Melanie Baybay
 * Last Modified: 10/29/17
 */
public class HottestMapper extends Mapper<LongWritable, Text, Text, Text>{
    // in-value: record -- 0. timestamp, 1. geohash, 40. temp
    // out-key: timestamp
    // out-value: geohash, temp

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t");
        String outValue = record[1] + "\t" + record[40];
        context.write(new Text(record[0]), new Text(outValue));
    }
}
