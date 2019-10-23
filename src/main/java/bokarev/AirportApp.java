package bokarev;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class AirportApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("AirportApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> flightsData = sc.textFile("/user/dima/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportsData = sc.textFile("/user/dima/L_AIRPORT_ID.csv");

        List<String> s = airportsData.collect();
        System.out.println(s.get(5));
    }
}
