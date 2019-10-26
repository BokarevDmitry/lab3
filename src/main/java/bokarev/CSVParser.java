package bokarev;

import org.apache.spark.api.java.JavaRDD;

public class CSVParser {
    public static String[] parseFlights(String s) {
        return s.split(",");
    }
    public static String[] parseAirports (String s) { return s.split("\",\""); }

    public static String removeQuotes (String s) {return s.replaceAll("[\"]", "");}

    public static String getAirportCode(String[] s) {return s[0];}
    public static String getAirportName (String[] s) { return s[1];}

    public static String getAirportOrigin (String[] s) {return s[11];}
    public static String getAirportDest (String[] s) {return s[14];}
    public static String getDelayTime (String[] s) {return s[18];}
    public static String getCancelStatus (String[] s) {return s[19];}

    public static JavaRDD<String> removeHeaders (JavaRDD<String> a) {
        String header = a.first();
        return a.filter(row -> !row.equals(header));
    }
}