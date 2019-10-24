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
        if (timeDelay>=1.0 || cancelStatus==1.0) {
            this.countDelayOrCancel = 1;
            System.out.println("DELAY1");
        } else {
            this.countDelayOrCancel = 0;
            System.out.println("DELAY0");
        }
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

    public Integer getCountRecords() {
        return countRecords;
    }

    public Integer getCountDelayOrCancel() {
        return countDelayOrCancel;
    }
}