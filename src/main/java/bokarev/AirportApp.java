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

        CSVParser.removeHeaders(flightsRDD);
        CSVParser.removeHeaders(airportsRDD);

        JavaPairRDD<String,String> airportDict = AirportMapper.mapAirports(airportsRDD);
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportDict.collectAsMap());

        JavaPairRDD<Tuple2, Tuple2> pairs = flightsRDD.mapToPair(
                (String s) -> {
                    String[] flightsInfo = CSVParser.parseFlights(s);
                    String airportOrigin = CSVParser.getAirportOrigin(flightsInfo);
                    String airportDest = CSVParser.getAirportDest(flightsInfo);
                    Float cancelStatus = Float.parseFloat(CSVParser.getCancelStatus(flightsInfo));

                    String timeDelay = CSVParser.getDelayTime(flightsInfo);
                    return new Tuple2<>(new Tuple2<>(
                            airportsBroadcasted.value().get(airportOrigin),
                            airportsBroadcasted.value().get(airportDest)),
                            new floatPair (timeDelay, cancelStatus));
                }
        )       .reduceByKey(
                        (floatPair a, floatPair b) -> new floatPair(
                        Math.max(a.getTimeDelay(), b.getTimeDelay()),
                        a.getCountRecords()+b.getCountRecords(),
                        a.getCountDelayOrCancel() + b.getCountDelayOrCancel()))
                .mapToPair(
                        (Tuple2<Tuple2<String,String>, floatPair> a) -> new Tuple2<>(
                                a._1,
                                new Tuple2<>(a._2.getTimeDelay(), a._2.getPercent())));


        //JavaPairRDD<Tuple2, Tuple2> last = Reducer.reduceAirports(pairs);
        //System.out.println(last.collect());
        pairs.saveAsTextFile("/user/dima/output");
    }
}
