package bokarev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair, Serializable {
    public  Float timeDelay;
    public  Float cancelStatus;

    public TextPair(Float timeDelay, Float second) {
        this.timeDelay = timeDelay;
        this.cancelStatus = second;
    }

    public TextPair(String timeDelay, String second){
        this.timeDelay =new Float(timeDelay);
        this.cancelStatus =new Float(second);
    }

    public Float getTimeDelay() {
        return timeDelay;
    }

    public Float getCancelStatus() {
        return cancelStatus;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPair textPair = (TextPair) o;
        return Objects.equals(timeDelay, textPair.timeDelay) &&
                Objects.equals(cancelStatus, textPair.cancelStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeDelay, cancelStatus);
    }

    @Override
    public String toString() {
        return timeDelay +"\t"+ cancelStatus;
    }
}