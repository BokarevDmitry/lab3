package bokarev;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportApp {
    SparkConf conf = new SparkConf().setAppName("AirportApp");
    JavaSparkContext sc = new JavaSparkContext(conf);

}
