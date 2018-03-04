package edu.usfca.cs.mr.greenenergy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * REDUCER: Filter the each category (solar, wind, solar+wind) to identify the top 3.
 *
 * Created By: Melanie Baybay
 * Last Modified: 11/13/17
 */
public class FilterEnergyReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(key.toString().equals("SOLAR")) {
            outputTop3Solar(values, context);
        } else if (key.toString().equals("WIND")) {
            outputTop3Wind(values, context);
        } else if(key.toString().equals("SOLAR+WIND")) {
            outputTop3SolarWind(values, context);
        }
    }

    private void outputTop3Solar(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double min1, min2, min3;
        min1 = min2 = min3 = Double.POSITIVE_INFINITY;
        ArrayList<String> top3SolarGeos = new ArrayList<>();
        for(Text val : values) {
            String[] value = val.toString().split("\t");
            String geo = value[0];
            double avgCloudCover = Double.parseDouble(value[1]);
            if(avgCloudCover < min1) {
                min3 = min2;
                min2 = min1;
                min1 = avgCloudCover;
                top3SolarGeos.add(0, geo);
                if(top3SolarGeos.size() > 3) {
                    top3SolarGeos.remove(3);
                }
            } else if (avgCloudCover < min2) {
                min3 = min2;
                min2 = avgCloudCover;
                top3SolarGeos.add(1, geo);
                if(top3SolarGeos.size() > 3) {
                    top3SolarGeos.remove(3);
                }
            } else if (avgCloudCover < min3) {
                min3 = avgCloudCover;
                top3SolarGeos.add(2, geo);
                if(top3SolarGeos.size() > 3) {
                    top3SolarGeos.remove(3);
                }
            }
        }

        Text key = new Text("SOLAR");
        context.write(key, new Text(top3SolarGeos.get(0) + "\t" + min1));
        context.write(key, new Text(top3SolarGeos.get(1) + "\t" + min2));
        context.write(key, new Text(top3SolarGeos.get(2) + "\t" + min3));
    }

    private void outputTop3Wind(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double max1, max2, max3, veg1, veg2, veg3;
        max1 = max2 = max3 = Double.NEGATIVE_INFINITY;
        veg1 = veg2 = veg3 = Double.NEGATIVE_INFINITY;
        ArrayList<String> top3WindGeos = new ArrayList<>();
        for(Text val : values) {
            String[] value = val.toString().split("\t");
            String geo = value[0];
            double avgWindSpeed = Double.parseDouble(value[1]);
            double avgVeg = Double.parseDouble(value[2]);
            if(avgWindSpeed > max1) {
                max3 = max2;
                max2 = max1;
                max1 = avgWindSpeed;
                // update top veg
                veg3 = veg2;
                veg2 = veg1;
                veg1 = avgVeg;
                top3WindGeos.add(0, geo);
                if(top3WindGeos.size() > 3) {
                    top3WindGeos.remove(3);
                }
            } else if (avgWindSpeed > max2) {
                max3 = max2;
                max2 = avgWindSpeed;
                // update top veg
                veg3 = veg2;
                veg2 = avgVeg;
                top3WindGeos.add(1, geo);
                if(top3WindGeos.size() > 3) {
                    top3WindGeos.remove(3);
                }
            } else if (avgWindSpeed > max3) {
                max3 = avgWindSpeed;
                veg3 = avgVeg;
                top3WindGeos.add(2, geo);
                if(top3WindGeos.size() > 3) {
                    top3WindGeos.remove(3);
                }
            }
        }

        Text key = new Text("WIND");
        context.write(key, new Text(top3WindGeos.get(0) + "\t" + max1 + "\t" + veg1));
        context.write(key, new Text(top3WindGeos.get(1) + "\t" + max2 + "\t" + veg2));
        context.write(key, new Text(top3WindGeos.get(2) + "\t" + max3 + "\t" + veg3));
    }

    private void outputTop3SolarWind(Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double wind1, wind2, wind3;
        double cloud1, cloud2, cloud3;
        double veg1, veg2, veg3;
        wind1 = wind2 = wind3 = Double.NEGATIVE_INFINITY;
        cloud1 = cloud2 = cloud3 = Double.POSITIVE_INFINITY;
        veg1 = veg2 = veg3 = Double.NEGATIVE_INFINITY;
        ArrayList<String> top3SolarWindGeos = new ArrayList<>();
        for(Text val : values) {
            String[] value = val.toString().split("\t");
            String geo = value[0];
            double avgWindSpeed = Double.parseDouble(value[1]);
            double avgVeg = Double.parseDouble(value[2]);
            double avgCloudCover = Double.parseDouble(value[3]);
            if (avgCloudCover <= cloud1) {
                if (avgWindSpeed > wind1) {
                    // update top wind speed
                    wind3 = wind2;
                    wind2 = wind1;
                    wind1 = avgWindSpeed;
                    // update top cloud cover
                    cloud3 = cloud2;
                    cloud2 = cloud1;
                    cloud1 = avgCloudCover;
                    // update top veg
                    veg3 = veg2;
                    veg2 = veg1;
                    veg1 = avgVeg;
                    top3SolarWindGeos.add(0, geo);
                    if (top3SolarWindGeos.size() > 3) {
                        top3SolarWindGeos.remove(3);
                    }
                } else if (avgCloudCover > wind2) {
                    // update top wind speed
                    wind3 = wind2;
                    wind2 = avgWindSpeed;
                    // update top cloud cover
                    cloud3 = cloud2;
                    cloud2 = avgCloudCover;
                    // update top veg
                    veg3 = veg2;
                    veg2 = avgVeg;
                    top3SolarWindGeos.add(1, geo);
                    if (top3SolarWindGeos.size() > 3) {
                        top3SolarWindGeos.remove(3);
                    }
                } else if (avgCloudCover > wind3) {
                    wind3 = avgCloudCover;
                    top3SolarWindGeos.add(2, geo);
                    if (top3SolarWindGeos.size() > 3) {
                        top3SolarWindGeos.remove(3);
                    }
                }
            }
        }

        Text key = new Text("SOLAR+WIND");
        context.write(key, new Text(top3SolarWindGeos.get(0) + "\t" + wind1 + "\t" + veg1 + "\t"  + cloud1));
        context.write(key, new Text(top3SolarWindGeos.get(1) + "\t" + wind2 + "\t" + veg2 + "\t"  + cloud2));
        context.write(key, new Text(top3SolarWindGeos.get(2) +  "\t" + wind3 + "\t" + veg3 + "\t"  + cloud3));
    }
}
