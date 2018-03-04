package edu.usfca.cs.mr.bayarea;

import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;


/**
 * MAPPER: If the record contains a geohash prefix that resides in the Bay Area,
 *      outputs: (key) month, (value) day, precipitation
 * Created By: Melanie Baybay
 * Last Modified: 11/1/17
 */
public class BayAreaMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<String> bayArea = initBayAreaGeoList();
        String[] record = value.toString().split("\t");
        String geoHash = record[1].substring(0, 4);

        if(bayArea.contains(geoHash)) {
            // out-key = timestamp (month)
            // extract timestamp as month and day
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(Long.parseLong(record[0]));
            SimpleDateFormat monthFmt = new SimpleDateFormat("yyyy-MM");
            SimpleDateFormat dayFmt = new SimpleDateFormat("yyyy-MM-dd");
            String month = monthFmt.format(cal.getTime());

            // out-val = day, precipitation
            String outVal = dayFmt.format(cal.getTime()) + "\t" + record[55];
            context.write(new Text(month), new Text(outVal));
        }
    }

    private ArrayList<String> initBayAreaGeoList(){
        float upperLat = (float)(38.32);
        float lowerLat = (float) (37.07);
        float lowerLon = (float)(-123.04);
        float upperLon = (float)(-121.63);

        float latGap = (upperLat - lowerLat) / 7;
        float lonGap = (lowerLon - upperLon) / 4;
        float topLeftLat = upperLat - (latGap/2);
        float topLeftLon = lowerLon - (lonGap/2);

        // get 7 latitudes
        ArrayList<Float> lats = new ArrayList<>();
        float lat = topLeftLat;
        for(int iLat = 0; iLat < 7; iLat++) {
            lats.add(lat);
            lat = lat - latGap;
        }
        // get 4 longitudes
        ArrayList<Float> longs = new ArrayList<>();
        float lon = topLeftLon;
        for(int iLon = 0; iLon < 4; iLon++) {
            longs.add(lon);
            lon = lon - lonGap;
        }


        ArrayList<String> geoHashes = new ArrayList<>();
        for(int i=0; i < 7; i++) {
            for(int j=0; j < 4; j++) {
                geoHashes.add(Geohash.encode(lats.get(i), longs.get(j), 4));
            }
        }
        return geoHashes;
    }
}
