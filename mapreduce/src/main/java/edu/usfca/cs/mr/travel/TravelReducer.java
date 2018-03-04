package edu.usfca.cs.mr.travel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.TreeMap;


/**
 * REDUCER: Outputs monthly average temperature and rain percentage for a single geohash-prefix
 *
 * Created By: Melanie Baybay
 * Last Modified: 11/5/17
 */
public class TravelReducer extends Reducer<Text, Text, Text, Text> {
    // in-key: geohash prefix
    // in-value: month (MM-yyyy), temp_surface, categorical_rain_yes1_no0_surface
    // out-key: geo-prefix
    // out-value: month, avg. temp, percentage of rainy days in month
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // index indicates month - 1
        TreeMap<String, Integer> months = new TreeMap<>();
        ArrayList<Double> avgTemps = new ArrayList<>(Collections.nCopies(12, (double) 0));
        ArrayList<Double> rainyPercent = new ArrayList<>(Collections.nCopies(12, (double) 0));
        ArrayList<Integer> nDays = new ArrayList<>(Collections.nCopies(12, 0));

        SimpleDateFormat monthYearFmt = new SimpleDateFormat("MM-yyyy");
        SimpleDateFormat monthFmt = new SimpleDateFormat("MM");
        // iterate thru all values - track avg. temp and number of rainy days
        for(Text val : values) {
            String[] record = val.toString().split("\t");
            // parse date-time
            Timestamp ts = new Timestamp(Long.parseLong(record[0]));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ts.getTime());
            // get month
            String month = monthYearFmt.format(cal.getTime());
            // parse temp and rainy-percentage
            double temp = Double.parseDouble(record[1]);
            int rainy = (int) Double.parseDouble(record[2]);
            // update data
            if(months.get(month) == null) {
                // get month index
                int iMonth = Integer.parseInt(monthFmt.format(cal.getTime())) - 1;
                months.put(month, iMonth);
                avgTemps.add(iMonth, temp);
                rainyPercent.add(iMonth, (double) rainy);
                nDays.add(iMonth, 1);
            } else {
                int iMonth = months.get(month);
                double avgTemp =  (avgTemps.get(iMonth) + temp) / 2;
                int n = nDays.get(iMonth) + 1;
                double rP = (rainyPercent.get(iMonth) + rainy) / n;
                avgTemps.add(iMonth, avgTemp);
                rainyPercent.add(iMonth, rP);
                nDays.add(iMonth, n);
            }
        }

        for(String month : months.keySet()) {
            context.write(key, new Text(month + "\t" + avgTemps.get(months.get(month))
                    + "\t" + rainyPercent.get(months.get(month))));
        }

    }

}
