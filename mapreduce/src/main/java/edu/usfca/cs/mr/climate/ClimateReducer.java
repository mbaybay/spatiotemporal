package edu.usfca.cs.mr.climate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * REDUCER: For a given month (in-key), calculates high temp, low temp, average temp, and average precipitation.
 *      output: (key) month, (value) high temp, low temp, average precipitation, average temp
 * Created By: Melanie Baybay
 * Last Modified: 11/13/17
 */
public class ClimateReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double highTemp = Double.NEGATIVE_INFINITY;
        double lowTemp = Double.POSITIVE_INFINITY;
        double avgTemp = 0;
        double avgPrecip = 0;
        double count = 0;

        for(Text val: values) {
            // parse value
            String[] value = val.toString().split("\t");
            double temp = Double.parseDouble(value[0]);
            double precip = Double.parseDouble(value[1]);
            // update high temp
            if(temp > highTemp) {
                highTemp = temp;
            }
            // update low temp
            if(temp < lowTemp) {
                lowTemp = temp;
            }
            // update avg temp
            avgTemp += temp;
            // update avg precip
            avgPrecip += precip;
            count += 1;
        }

        avgTemp /= count;
        avgPrecip /= count;

        context.write(key, new Text(Double.toString(highTemp) + "\t" + Double.toString(lowTemp)
                + "\t" + Double.toString(avgPrecip) + "\t" + Double.toString(avgTemp)));
    }
}
