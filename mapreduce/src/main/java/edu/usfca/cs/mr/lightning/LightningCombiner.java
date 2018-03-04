package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * COMBINER: Sum number of lightning_surface > 0 records, for quicker calculation later.
 *
 * Created By: Melanie Baybay
 * Last Modified: 10/31/17
 */
public class LightningCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable val : values) {
            count += val.get();
        }
        context.write(new Text(key), new IntWritable(count));
    }
}
