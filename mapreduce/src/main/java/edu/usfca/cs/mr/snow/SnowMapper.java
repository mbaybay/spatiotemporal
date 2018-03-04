package edu.usfca.cs.mr.snow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * MAPPER: Identifies records with snow_depth > 0.
 *      output: (key) geohash, (value) month
 * Created By: Melanie Baybay
 * Last Modified: 10/29/17
 */
public class SnowMapper extends Mapper<LongWritable, Text, Text, Text>{
    //out-key: geohash
    //out-value: date
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input: record in file
        String[] fields = value.toString().split("\t");
        // extract snow_depth
        double snow_depth = Double.parseDouble(fields[50]);
        if(snow_depth > 0) {
            // extract timestamp as yyyy-MM
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(Long.parseLong(fields[0]));
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM");
            String date = fmt.format(cal.getTime());

            // add to output
            context.write(new Text(fields[1]), new Text(date));
        }
    }
}
