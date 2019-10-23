package bokarev;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportApp {
    SparkConf conf = new SparkConf().setAppName("AirportApp");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> flightsData = sc.textFile("/src/main/resources/664600583_T_ONTIME_sample.csv");
    JavaRDD<String> airportsData = sc.textFile("/src/main/resources/L_AIRPORT_ID.csv");

}
