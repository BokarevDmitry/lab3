package bokarev;

import java.io.Serializable;
import java.util.Objects;


public class floatPair implements Serializable {
    public  Float timeDelay;
    public  Float cancelStatus;
    public  Integer countRecords;
    public  Integer countDelayOrCancel;

    public floatPair(Float timeDelay, Float cancelStatus) {
        this.timeDelay = timeDelay;
        this.cancelStatus = cancelStatus;
        this.countRecords = 1;
        if (timeDelay>0 && cancelStatus>0)
            this.countDelayOrCancel = 1;
    }

    public floatPair(Float timeDelay, Integer countRecords, Integer countDelayOrCancel) {
        this.timeDelay = timeDelay;
        this.countRecords = countRecords;
        this.countDelayOrCancel = countDelayOrCancel;
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