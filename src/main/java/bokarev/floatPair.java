package bokarev;

import java.io.Serializable;
import java.util.Objects;


public class floatPair implements Serializable {
    public  Float timeDelay;
    public  Float cancelStatus;
    public

    public floatPair(Float timeDelay, Float second) {
        this.timeDelay = timeDelay;
        this.cancelStatus = second;
    }

    public floatPair(String timeDelay, String second){
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
        floatPair textPair = (floatPair) o;
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