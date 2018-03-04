package edu.usfca.cs.mr.greenenergy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * REDUCER: Output all viable solar, wind, and solar+wind farms for the data.
 *          possible outputs:
 *          (key) WIND      ,(value) geo4, avg-annual-windspeed, avg-vegetation
 *          (key) SOLAR     ,(value) geo5, avg-cloud-cover
 *          (key) SOLAR+WIND,(value) geo4, avg-annual-windspeed, avg-vegetation, avg-cloud-cover
 *
 * Created By: Melanie Baybay
 * Last Modified: 11/12/17
 */
public class GreenEnergyReducer extends Reducer<Text, Text, Text, Text> {
    private final double CLOUD_COVER_THRES = 0.3;
    private final double VEG_THRES = 0.1;
    private final double WIND_SPEED_MIN = 8;
    private final double WIND_SPEED_MAX = 20;
    // in-KEY: geo4
    // in-VAL: geo5, date, windspeed, vegetation, clouds

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // build data-structure
        LinkedHashMap<String, LinkedHashMap<String, Double[]>> geo5DateInfoMap = buildFromValues(values);
        // calculate averages
        LinkedHashMap<String, Double[]> geo5InfoAvgs = computeGeo5Averages(geo5DateInfoMap);
        double[] geo4Avgs = computeGeo4Averages(geo5InfoAvgs);
        // SOLAR VERIFICATION
        // boolean list for geo5 keyset
        ArrayList<Boolean> solarReady = verifySolar(geo5InfoAvgs);
        boolean windReady = verifyWind(geo4Avgs);
        boolean solarWindReady = verifySolarAndWind(solarReady, windReady);

        // OUTPUT
        outputSolar(geo5InfoAvgs, solarReady, context);

        if(windReady) {
            outputWind(key, geo4Avgs, context);
        }

        if(solarWindReady) {
            outputSolarWind(key, geo4Avgs, context);
        }

    }

    private LinkedHashMap<String, LinkedHashMap<String, Double[]>> buildFromValues(Iterable<Text> values) {
        LinkedHashMap<String, LinkedHashMap<String, Double[]>> geo5DateInfoMap = new LinkedHashMap<>();

        for(Text val : values) {
            // in-VAL: geo5, date, windspeed, vegetation, clouds
            String[] value = val.toString().split("\t");
            String geo5 = value[0];
            String date = value[1];
            double windSpeed = Double.parseDouble(value[2]);
            double veg = Double.parseDouble(value[3]);
            double clouds = Double.parseDouble(value[4]);

            if(geo5DateInfoMap.containsKey(geo5)) {
                if(geo5DateInfoMap.get(geo5).containsKey(date)) {
                    Double[] oldInfo = geo5DateInfoMap.get(geo5).get(date);
                    double newWindSpeed = oldInfo[0] + windSpeed;
                    double newVeg = oldInfo[1] + veg;
                    double newClouds = oldInfo[2] + clouds;
                    double newCount = oldInfo[3] + 1.0;
                    Double[] info = {newWindSpeed, newVeg, newClouds, newCount};
                    geo5DateInfoMap.get(geo5).put(date, info);
                } else {
                    Double[] info = {windSpeed, veg, clouds, 1.0};
                    geo5DateInfoMap.get(geo5).put(date, info);
                }
            } else {
                Double[] info = {windSpeed, veg, clouds, 1.0};
                // key: date
                // val: windspead, veg, clouds, nObservations
                LinkedHashMap<String, Double[]> dateInfoMap = new LinkedHashMap<>();
                dateInfoMap.put(date, info);
                geo5DateInfoMap.put(geo5, dateInfoMap);
            }
        }
        return geo5DateInfoMap;
    }

    private LinkedHashMap<String, Double[]> computeGeo5Averages(LinkedHashMap<String, LinkedHashMap<String, Double[]>> infoMap) {
        LinkedHashMap<String, Double[]> annualAverages = new LinkedHashMap<>();
        for(String geo5 : infoMap.keySet()) {
            double avgWindSpeed = 0;
            double avgVeg = 0;
            double avgClouds = 0;
            double n = infoMap.get(geo5).size();

            for(String date : infoMap.get(geo5).keySet()) {
                Double[] info = infoMap.get(geo5).get(date);
                double count = info[3];
                avgWindSpeed += (info[0] / count);
                avgVeg += (info[1] / count);
                avgClouds += (info[2] / count);
            }

            avgWindSpeed /= n;
            avgVeg /= n;
            avgClouds /= n;
            Double[] averages = {avgWindSpeed, avgVeg, avgClouds};
            annualAverages.put(geo5, averages);
        }

        return annualAverages;
    }

    private double[] computeGeo4Averages(LinkedHashMap<String, Double[]> infoMap) {
        double avgWindSpeed = 0;
        double avgVeg = 0;
        double avgClouds = 0;
        double n = infoMap.size();
        Double[] info;
        for(String geo5 : infoMap.keySet()) {
            info = infoMap.get(geo5);
            avgWindSpeed += info[0];
            avgVeg += info[1];
            avgClouds += info[2];
        }
        avgWindSpeed /= n;
        avgVeg /= n;
        avgClouds /= n;

        double[] avgs = {avgWindSpeed, avgVeg, avgClouds};

        return avgs;
    }

    private ArrayList<Boolean> verifySolar(LinkedHashMap<String, Double[]> infoMap) {
        ArrayList<Boolean> solarReady = new ArrayList<>();
        for(String geo5 : infoMap.keySet()) {
            if(infoMap.get(geo5)[2] < CLOUD_COVER_THRES) {
                solarReady.add(Boolean.TRUE);
            } else {
                solarReady.add(Boolean.FALSE);
            }
        }
        return solarReady;
    }

    private boolean verifyWind(double[] averages) {
        double avgWindSpeed = averages[0];
        double avgVeg = averages[1];

        if(((avgWindSpeed >= WIND_SPEED_MIN) & (avgWindSpeed < WIND_SPEED_MAX)) & (avgVeg <= VEG_THRES)) {
            return true;
        }
        return false;
    }

    private boolean verifySolarAndWind(ArrayList<Boolean> solarReady, boolean windReady) {
        if(windReady) {
            for(Boolean solar : solarReady) {
                if(!solar) {
                    return false;
                }
            }
            // windReady and all solarReady
            return true;
        }
        return false;
    }

    private void outputSolar(LinkedHashMap<String, Double[]> geo5InfoAvgs, ArrayList<Boolean> solarReady, Context ctx)
            throws IOException, InterruptedException{
        int i = 0;
        for(String geo5 : geo5InfoAvgs.keySet()){
            if(solarReady.get(i)) {
                Double[] info = geo5InfoAvgs.get(geo5);
                ctx.write(new Text("SOLAR"), new Text(geo5 + "\t" + Double.toString(info[2])));
            }
            i++;
        }
    }

    private void outputWind(Text key, double[] geo4Avgs, Context ctx)
            throws IOException, InterruptedException{
        ctx.write(new Text("WIND"), new Text(key.toString() + "\t" + geo4Avgs[0] + "\t" + geo4Avgs[1]));
    }

    private void outputSolarWind(Text key, double[] geo4Avgs, Context ctx)
            throws IOException, InterruptedException{
        ctx.write(new Text("SOLAR+WIND"), new Text(key.toString() + "\t"
                + geo4Avgs[0] + "\t" + geo4Avgs[1] + "\t" + geo4Avgs[2]));
    }
}
