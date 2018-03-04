package edu.usfca.cs.mr.climate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * MAPPER: If the record contains a geohash prefix that matches the one provided by the user,
 *      it outputs: (key) month, (value) temp, precipitation
 * Created By: Melanie Baybay
 * Last Modified: 11/13/17
 */
public class ClimateMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // extract user-defined GEO
        Configuration conf = context.getConfiguration();
        String userGeo = conf.get("geo");
        int precision = userGeo.length();
        // parse record
        String[] record = value.toString().split("\t");
        String recordGeo = record[1].substring(0, precision);
        if(userGeo.equals(recordGeo)) {
            String month = parseMonth(record[0]);
            String temp = convertToFahrenheit(record[40]);
            String precip = convertToInches(record[55]);
            context.write(new Text(month), new Text(temp + "\t" + precip));
        }
    }

    private String parseMonth(String timestamp) {
        SimpleDateFormat monthFmt = new SimpleDateFormat("MM");
        Timestamp ts = new Timestamp(Long.parseLong(timestamp));
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(ts.getTime());
        String month = monthFmt.format(cal.getTime());
        return month;
    }

    private String convertToFahrenheit(String kelvins) {
        double k = Double.parseDouble(kelvins);
        double f = 1.8 * (k - 273) + 32;
        return Double.toString(f);
    }

    private String convertToInches(String precip){
        // http://www.metric-conversions.org/pressure/kilogram-force-per-square-meter-to-inches-of-water.htm
        double kgm2 = Double.parseDouble(precip);
        double precipInches = kgm2 * 0.039370;
        return Double.toString(precipInches);
    }
}
