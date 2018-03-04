package edu.usfca.cs.mr.bayarea;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * REDUCER: Calculates monthly average precipitation:
 *      (key) month, (value) day, avg. precipitable water
 * Created By: Melanie Baybay
 * Last Modified: 11/1/17
 */
public class BayAreaReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    // in-key: date (yyyy-MM)
    // in-val: date (yyyy-MM-dd), precipitable water for this day, count
    // out-key: date (yyyy-MM)
    // out-val: avg precipitable water for this month
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // store average for each day
        HashMap<String, Double> dailySums = new HashMap<>();
        HashMap<String, Integer> dailyRecordCount = new HashMap<>();
        // parse each value
        for(Text val : values) {
            String[] record = val.toString().split("\t");
            // extract date, sum, nRecords
            String date = record[0];
            double precipSum = Double.parseDouble(record[1]);
            // store info
            if(dailySums.containsKey(date)) {
                dailySums.put(date, dailySums.get(date) + precipSum);
                dailyRecordCount.put(date, dailyRecordCount.get(date) + 1);
            } else {
                dailySums.put(date, precipSum);
                dailyRecordCount.put(date, 1);
            }
        }

        // compute average for each day and add to total month average
        double monthAvg = 0;
        for(String day : dailySums.keySet()) {
            monthAvg += dailySums.get(day) / dailyRecordCount.get(day);
        }

        // divide sum by number of days
        monthAvg = monthAvg / dailySums.size();

        context.write(key, new DoubleWritable(monthAvg));
    }
}
