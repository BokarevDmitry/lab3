package bokarev;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportApp {
    public static void main(String[] args) throws Exception {
        String DESCRIPTION_LINE = "ARR_DELAY_NEW";
        String CANCEL_LINE = "CANCELLED";

        SparkConf conf = new SparkConf().setAppName("AirportApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsRDD = sc.textFile("/user/dima/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportsRDD = sc.textFile("/user/dima/L_AIRPORT_ID.csv");

        JavaPairRDD<Tuple2, floatPair> pairs = flightsRDD.mapToPair(
                (String s) -> {
                    String[] flightsInfo = CSVParser.parseFlights(s);
                    String airportOrigin = CSVParser.getAirportOrigin(flightsInfo);
                    String airportDest = CSVParser.getAirportDest(flightsInfo);
                    if (!flightsInfo[18].contains(DESCRIPTION_LINE)
                            && !flightsInfo[19].contains(CANCEL_LINE))
                           // && !flightsInfo[18].isEmpty()
                           // && Float.parseFloat(flightsInfo[18])>0)
                    {
                        Float cancelStatus = Float.parseFloat(CSVParser.getCancelStatus(flightsInfo));
                        if (!flightsInfo[18].isEmpty()) {
                            Float timeDelay = Float.parseFloat(CSVParser.getDelayTime(flightsInfo));
                            return new Tuple2<>(new Tuple2<>(airportOrigin, airportDest),
                                    new floatPair (timeDelay, cancelStatus));
                         } else {
                            return new Tuple2<>(new Tuple2<>(airportOrigin, airportDest),
                                    new floatPair ((float)0, cancelStatus));
                        }

                    }
                    return new Tuple2<>(new Tuple2<>("",""),
                            new floatPair ((float)0, (float)0));
                }
        );

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

        System.out.println(last.collect());

        

    }
}
