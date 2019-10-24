package bokarev;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class AirportApp {
    public static void main(String[] args) throws Exception {
        String DESCRIPTION_LINE = "ARR_DELAY_NEW";

        SparkConf conf = new SparkConf().setAppName("AirportApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsRDD = sc.textFile("/user/dima/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportsRDD = sc.textFile("/user/dima/L_AIRPORT_ID.csv");

        //flightsRDD.map(AirportApp.map);
        JavaPairRDD<Tuple2, Float> pairs = flightsRDD.mapToPair(
                (String s) -> {
                    String[] columns = CSVParser.parseFlights(s);
                    //String airportsPair = columns[11] + "-" + columns[14];
                    String airportOrigin = CSVParser.getAirportOrigin(columns);
                    String airportDest = CSVParser.getAirportDest(columns);
                    if (!columns[18].contains(DESCRIPTION_LINE) && !columns[18].isEmpty() && Float.parseFloat(columns[18])>0) {
                        Float timeDelay = Float.parseFloat(columns[18]);
                        //String cancelStatus = columns[19];
                        return new Tuple2<>(new Tuple2<>(airportOrigin, airportDest), timeDelay);
                    }
                    return new Tuple2<>(new Tuple2<>("",""), (float)0);
                }
        );

        JavaPairRDD<Tuple2, Float> counts = pairs.reduceByKey(
                (Float a, Float b) -> Math.max(a,b)
        );

        System.out.println(counts.collect());


        //List<String> flightsList = flightsRDD.collect();
        //List<String> airportList = airportsRDD.collect();
        //System.out.println(s.get(5));
    }
}
