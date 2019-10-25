package bokarev;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class AirportMapper {

    public static JavaPairRDD<String,String> mapAirports (JavaRDD<String> airportsRDD) {
        return airportsRDD.mapToPair(
                (String s) -> {
                    String airportsInfo[] = CSVParser.parseAirports(s);
                    String airportCode = CSVParser.getAirCode(airportsInfo);
                    String airportName = CSVParser.getAirportName(airportsInfo);
                    return new Tuple2<>(
                            CSVParser.removeQuotes(airportCode),
                            CSVParser.removeQuotes(airportName));
                }
        );
    }


}
