package bokarev;

import java.io.Serializable;
import java.util.Objects;


public class maxAndPercentPair implements Serializable {
    public  Float maxDelay;
    public  Float percent;

    public maxAndPercentPair(Float maxDelay, Integer countRecords, Integer countDelayOrCancel) {
        this.maxDelay = maxDelay;
        this.percent = (float)countDelayOrCancel*100/(float)countRecords;
    }
}