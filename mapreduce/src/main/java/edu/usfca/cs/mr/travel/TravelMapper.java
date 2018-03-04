package edu.usfca.cs.mr.travel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * MAPPER: If record contains geohash-prefix that is in the user-provided list of travel regions,
 *      output: (key) geohash-prefix (value) month (MM-yyyy), temp_surface, categorical_rain_yes1_no0_surface
 * Created By: Melanie Baybay
 * Last Modified: 11/5/17
 */
public class TravelMapper extends Mapper<LongWritable, Text, Text, Text> {
    // out-key: geohash prefix
    // out-value: month (MM-yyyy), temp_surface, categorical_rain_yes1_no0_surface

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> regions = initRegions(context);
        String[] record = value.toString().split("\t");
        String geo = record[1].substring(0, regions.get(0).length());
        String tempF = convertToFahrenheit(record[40]);
        if(regions.contains(geo)) {
            context.write(new Text(geo), new Text(record[0] + "\t" + tempF + "\t" + record[29]));
        }
    }

    private List<String> initRegions(Context ctx) {
        Configuration conf = ctx.getConfiguration();
        List<String> regions = new ArrayList<>();
        regions.add(conf.get("geo1"));
        regions.add(conf.get("geo2"));
        regions.add(conf.get("geo3"));
        regions.add(conf.get("geo4"));
        regions.add(conf.get("geo5"));

        return regions;
    }

    private String convertToFahrenheit(String kelvins) {
        double k = Double.parseDouble(kelvins);
        double f = 1.8 * (k - 273) + 32;
        return Double.toString(f);
    }

}
