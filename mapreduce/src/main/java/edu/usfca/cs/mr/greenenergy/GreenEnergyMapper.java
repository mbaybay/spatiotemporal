package edu.usfca.cs.mr.greenenergy;

import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * MAPPER: Calculate average annual wind speeds, vegetation, and cloud cover for a geohash region (precision 5 for
 *      solar, precision 4 for wind).
 *      outputs: (key) geo w/ 4 precision, (value) geo w/ 5 precision, month, windspeed, vegetation, cloud cover
 * Created By: Melanie Baybay
 * Last Modified: 11/12/17
 */
public class GreenEnergyMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<String> unitedStatesGeos = initUSGeoList();
        String[] record = value.toString().split("\t");

        String geo2 = record[1].substring(0, 2);

        // VERIFY GEOSHASH IN US
        if(unitedStatesGeos.contains(geo2)) {
            // parse DATE in month format
            SimpleDateFormat monthFmt = new SimpleDateFormat("MM-yyyy");
            Timestamp ts = new Timestamp(Long.parseLong(record[0]));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ts.getTime());
            String month = monthFmt.format(cal.getTime());
            // parse GEOHASH in 4 and 5 precision
            String geo4 = record[1].substring(0, 4);
            String geo5 = record[1].substring(0, 5);
            // calc WIND SPEED from U-COMPONENT V-COMPONENT OF WIND SPEED
            double windSpeed = calcWindSpeed(Double.parseDouble(record[53]), Double.parseDouble(record[37]));
            // parse VEGETATION_SURFACE
            String veg = record[28];
            // parse TOTAL_CLOUD_COVER_ENTIRE_ATMOSPHERE
            String clouds = record[16];

            // KEY: geo4, VAL: geo5, date, windspeed, vegetation, clouds
            context.write(new Text(geo4), new Text(geo5 + "\t" + month + "\t" + windSpeed + "\t" + veg + "\t" + clouds));
        }
    }

    private double calcWindSpeed(double u, double v) {
        double sum = Math.pow(u, 2) + Math.pow(v, 2);
        return Math.sqrt(sum);
    }


    private ArrayList<String> initUSGeoList(){
        float upperLat = (float)(49.00);
        float lowerLat = (float) (24.54);
        float lowerLon = (float)(-66.97);
        float upperLon = (float)(-124.45);

        float latGap = (upperLat - lowerLat) / 5;
        float lonGap = (lowerLon - upperLon) / 4;
        float topLeftLat = upperLat - (latGap/2);
        float topLeftLon = lowerLon - (lonGap/2);

        // get 7 latitudes
        ArrayList<Float> lats = new ArrayList<>();
        float lat = topLeftLat;
        for(int iLat = 0; iLat < 5; iLat++) {
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
        for(int i=0; i < 5; i++) {
            for(int j=0; j < 4; j++) {
                geoHashes.add(Geohash.encode(lats.get(i), longs.get(j), 2));
            }
        }

        return geoHashes;
    }

}
