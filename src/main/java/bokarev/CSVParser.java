package bokarev;
import org.apache.hadoop.io.Text;

public class CSVParser {
    public static String[] parseFlights(Text s) {
        return s.toString().split(",");
    }
    public static String[] parseAirports (Text s) {
        return s.toString().split("\",\"");
    }

    public static String removeQuotes (String s) {return s.replaceAll("[\"]", "");}

    public static String getAirCode (String[] s) {return s[0];}
    public static String getAirportName (String[] s) { return s[1];}

    public static String getAirportCode (String[] s) {return s[14];}
    public static String getDelayTime (String[] s) {return s[18];}
}