package bokarev;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class Mappers {

    public static JavaPairRDD<String,String> mapAirports (JavaRDD<String> airportsRDD) {
        return airportsRDD
                .mapToPair(
                (String s) -> {
                    String airportsInfo[] = CSVParser.parseAirports(s);
                    String airportCode = CSVParser.getAirportCode(airportsInfo);
                    String airportName = CSVParser.getAirportName(airportsInfo);
                    return new Tuple2<>(
                            CSVParser.removeQuotes(airportCode),
                            CSVParser.removeQuotes(airportName));
                }
        );
    }

    public static JavaPairRDD<Tuple2, Tuple2> mapFlights (JavaRDD<String> flightsRDD, Broadcast<Map<String, String>> airportsBroadcasted) {
        return flightsRDD
                .mapToPair(
                        (String s) -> {
                            String[] flightsInfo = CSVParser.parseFlights(s);
                            String airportOrigin = CSVParser.getAirportOrigin(flightsInfo);
                            String airportDest = CSVParser.getAirportDest(flightsInfo);
                            Float cancelStatus = Float.parseFloat(CSVParser.getCancelStatus(flightsInfo));
                            String timeDelay = CSVParser.getDelayTime(flightsInfo);
                            return new Tuple2<>(new Tuple2<>(
                                    airportsBroadcasted.value().get(airportOrigin),
                                    airportsBroadcasted.value().get(airportDest)),
                                    new Storage(timeDelay, cancelStatus));
                        }
                )
                .reduceByKey(
                        (Storage a, Storage b) -> new Storage(
                                Math.max(a.getTimeDelay(), b.getTimeDelay()),
                                a.getCountRecords()+b.getCountRecords(),
                                a.getCountDelayOrCancel() + b.getCountDelayOrCancel()))
                .mapToPair(
                        (Tuple2<Tuple2<String,String>, Storage> a) -> new Tuple2<>(
                                a._1,
                                new Tuple2<>(a._2.getTimeDelay(), a._2.getPercent())));
    }
}
