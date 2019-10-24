package bokarev;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportApp {
    public static void main(String[] args) throws Exception {
        String DESCRIPTION_LINE = "ARR_DELAY_NEW";

        SparkConf conf = new SparkConf().setAppName("AirportApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsRDD = sc.textFile("/user/dima/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportsRDD = sc.textFile("/user/dima/L_AIRPORT_ID.csv");

        //flightsRDD.map(AirportApp.map);
        JavaPairRDD<Tuple2, floatPair> pairs = flightsRDD.mapToPair(
                (String s) -> {
                    String[] flightsInfo = CSVParser.parseFlights(s);
                    String airportOrigin = CSVParser.getAirportOrigin(flightsInfo);
                    String airportDest = CSVParser.getAirportDest(flightsInfo);
                    Float cancelStatus = Float.parseFloat(CSVParser.getCancelStatus(flightsInfo));


                    if (!flightsInfo[18].contains(DESCRIPTION_LINE) && !flightsInfo[18].isEmpty() && Float.parseFloat(flightsInfo[18])>0) {
                        Float timeDelay = Float.parseFloat(CSVParser.getDelayTime(flightsInfo));
                        Float[] valueInfo = {timeDelay, cancelStatus};
                        return new Tuple2<>(new Tuple2<>(airportOrigin, airportDest),
                               new floatPair (timeDelay, cancelStatus));
                    }
                    return new Tuple2<>(new Tuple2<>("",""), new floatPair ((float)0, (float)0));
                }
        );

        JavaPairRDD<Tuple2, floatPair> maxDelayTime = pairs.reduceByKey(
                (floatPair a, floatPair b) -> {
                    a.countDelayOrCancel += b.countDelayOrCancel;
                    a.countRecords+=1;
                    //return Math.max(a,b);
                    return new floatPair( Math.max(a.getTimeDelay(), b.getTimeDelay()),
                            a.countRecords++,
                            a.countDelayOrCancel + b.countDelayOrCancel);
                }
        );

        JavaPairRDD<Tuple2, floatPair> percentPairs = maxDelayTime





        System.out.println(maxDelayTime.collect());


        //List<String> flightsList = flightsRDD.collect();
        //List<String> airportList = airportsRDD.collect();
        //System.out.println(s.get(5));
    }
}
