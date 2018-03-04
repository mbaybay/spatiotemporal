package edu.usfca.cs.mr.superhot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * COMBINER: Select record with max temperature to pass to reducer.
 * Created By: Melanie Baybay
 * Last Modified: 10/30/17
 */
public class HottestCombiner extends Reducer<Text, Text, Text, Text> {
    // in-key: timestamp
    // in-value: geohash,temp
    // out-key: "hot"
    // out-value: geohash,temp,timestamp
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double maxTemp = Double.NEGATIVE_INFINITY;
        String maxOutputValue = "";

        for (Text val : values) {
            String[] record = val.toString().split("\t");
            String geohash = record[0];
            double temp = Double.parseDouble(record[1]);
            if(temp > maxTemp){
                maxTemp = temp;
                // store geohash and temp
                maxOutputValue = geohash + "\t" + maxTemp + "\t" + key.toString();
            }
        }

        context.write(new Text("hot"), new Text(maxOutputValue));
    }
}
