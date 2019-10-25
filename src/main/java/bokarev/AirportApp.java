package bokarev;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;
import org.apache.spark.util.LongAccumulator;

public class AirportApp {
    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
        if (hdfs.exists(new Path("hdfs://localhost:9000/user/dima/output"))) {
            hdfs.delete(new Path("hdfs://localhost:9000/user/dima/output"), true);
        }

        SparkConf conf = new SparkConf().setAppName("AirportApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsRDD = sc.textFile("/user/dima/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportsRDD = sc.textFile("/user/dima/L_AIRPORT_ID.csv");

        String headerFlights = flightsRDD.first();
        String headerAirports = airportsRDD.first();

        flightsRDD = flightsRDD.filter(row -> !row.equals(headerFlights));
        airportsRDD = airportsRDD.filter(row -> !row.equals(headerAirports));

        JavaPairRDD<String,String> airportDict = AirportMapper.mapAirports(airportsRDD);
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportDict.collectAsMap());

        JavaPairRDD<Tuple2, floatPair> pairs = flightsRDD.mapToPair(
                (String s) -> {
                    String[] flightsInfo = CSVParser.parseFlights(s);
                    String airportOrigin = CSVParser.getAirportOrigin(flightsInfo);
                    String airportDest = CSVParser.getAirportDest(flightsInfo);
                    Float cancelStatus = Float.parseFloat(CSVParser.getCancelStatus(flightsInfo));
                    if (!flightsInfo[18].isEmpty()) {
                        Float timeDelay = Float.parseFloat(CSVParser.getDelayTime(flightsInfo));
                        return new Tuple2<>(new Tuple2<>(
                                airportsBroadcasted.value().get(airportOrigin),
                                airportsBroadcasted.value().get(airportDest)),
                                new floatPair (timeDelay, cancelStatus));
                    }   else {
                        return new Tuple2<>(new Tuple2<>(
                                airportsBroadcasted.value().get(airportOrigin),
                                airportsBroadcasted.value().get(airportDest)),
                                new floatPair ((float)0, cancelStatus));
                        }
                }
        );

        //final org.apache.spark.util.LongAccumulator accum = sc.sc().longAccumulator();

        JavaPairRDD<Tuple2, floatPair> maxDelayTime = pairs.reduceByKey(
                (floatPair a, floatPair b) -> {
                    return new floatPair( Math.max(a.getTimeDelay(), b.getTimeDelay()),
                            a.countRecords+b.countRecords,
                            a.countDelayOrCancel + b.countDelayOrCancel);
                }
        );


        JavaPairRDD<Tuple2, maxAndPercentPair> percentPairs = maxDelayTime.mapToPair(
                (Tuple2<Tuple2, floatPair> a) -> {
                    return new Tuple2<>(a._1, new maxAndPercentPair(a._2.getTimeDelay(), a._2.getCountRecords(), a._2.getCountDelayOrCancel()));
                }
        );

        JavaPairRDD<Tuple2, Tuple2> last = percentPairs.mapToPair(
                (Tuple2<Tuple2, maxAndPercentPair> a) -> new Tuple2<>(a._1, new Tuple2<>(a._2.maxDelay, a._2.percent))
        );

        //System.out.println(last.collect());
        last.saveAsTextFile("/user/dima/output");
    }
}
