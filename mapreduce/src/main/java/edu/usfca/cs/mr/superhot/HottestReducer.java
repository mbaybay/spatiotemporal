package edu.usfca.cs.mr.superhot;

import edu.usfca.cs.mr.util.Coordinates;
import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * REDUCER: Identify hottest observed temperature.
 *
 * Created By: Melanie Baybay
 * Last Modified: 10/29/17
 */
public class HottestReducer extends Reducer<Text, Text, Text, DoubleWritable>{
    //in-key: timestamp
    //in-value: geohash,temp,timestamp

    //out-key: location, timestamp
    //out-value: temp

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double maxTemp = Double.NEGATIVE_INFINITY;
        String maxOutputKey = "";

        for (Text val : values) {
            String[] record = val.toString().split("\t");
            String geohash = record[0];
            double temp = Double.parseDouble(record[1]);
            String ts = record[2];
            if(temp > maxTemp){
                maxTemp = temp;
                // convert geohash
                Coordinates c = Geohash.decodeHash(geohash).getCenterPoint();
                // extract timestamp as yyyy-MM
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(Long.parseLong(ts.toString()));
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
                String date = fmt.format(cal.getTime());
                // store coordinates and date in
                maxOutputKey = c.toString() + "\t" + date;
            }
        }

        context.write(new Text(maxOutputKey), new DoubleWritable(maxTemp));
    }
}
