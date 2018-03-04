package edu.usfca.cs.mr.lightning;

import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * REDUCER: Sum number of lighting_surface records.
 *      output: (key) coordinates (value) number of lightning_surface > 0 records
 * Created By: Melanie Baybay
 * Last Modified: 10/31/17
 */
public class LightningReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String outKey = Geohash.decodeHash(key.toString()).getCenterPoint().toString();
        int count = 0;
        for(IntWritable val : values) {
            count += val.get();
        }
        context.write(new Text(outKey), new IntWritable(count));
    }
}
