package bokarev;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class Reducer {
    public static JavaPairRDD<Tuple2, Tuple2> reduceAirports (JavaPairRDD<Tuple2, floatPair> airportsMapped) {
        return airportsMapped.reduceByKey(
                (floatPair a, floatPair b) -> new floatPair(
                        Math.max(a.getTimeDelay(), b.getTimeDelay()),
                        a.getCountRecords()+b.getCountRecords(),
                        a.getCountDelayOrCancel() + b.getCountDelayOrCancel()))
                .mapToPair(
                        (Tuple2<Tuple2, floatPair> a) -> new Tuple2<>(
                                a._1,
                                new Tuple2<>(a._2.getTimeDelay(), a._2.getPercent())));
    }
}
