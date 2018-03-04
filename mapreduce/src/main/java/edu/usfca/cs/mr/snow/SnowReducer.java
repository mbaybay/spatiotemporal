package edu.usfca.cs.mr.snow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * REDUCER: Sums records to identify which have snow for all months of the year.
 *
 * Created By: Melanie Baybay
 * Last Modified: 10/29/17
 */
public class SnowReducer extends Reducer<Text, Text, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key: same geohash
        // value: timestamp
        Set<String> dates = new HashSet<>();

        for(Text val : values) {
            dates.add(val.toString());
        }

        if (dates.size() >= 12) {
            context.write(key, new Text(""));
        }

    }
}
