package bokarev;

import java.io.Serializable;

public class floatPair implements Serializable {
    private   Float timeDelay;
    private   Float cancelStatus;
    private   Integer countRecords;
    private   Integer countDelayOrCancel;
    private   Float percent;

    public floatPair(String timeDelay, Float cancelStatus) {
        if (!timeDelay.isEmpty()) {
            this.timeDelay = Float.parseFloat(timeDelay);
        } else this.timeDelay = (float)0;
        this.cancelStatus = cancelStatus;
        this.countRecords = 1;
        if (this.timeDelay>=1.0 || this.cancelStatus==1.0) {
            this.countDelayOrCancel = 1;
        } else this.countDelayOrCancel = 0;
    }

    public floatPair(Float timeDelay, Integer countRecords, Integer countDelayOrCancel) {
        this.timeDelay = timeDelay;
        this.countRecords = countRecords;
        this.countDelayOrCancel = countDelayOrCancel;
        this.percent = (float)countDelayOrCancel*100/(float)countRecords;
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

    public Float getPercent() {
        return percent;
    }
}