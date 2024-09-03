package dev.morling.onebrc;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

public class ValueHolder {
    double max;
    double min;
    double mean;
    volatile int count = 1;
    int  sumForMean = 0;

    AtomicInteger countAsync = new AtomicInteger(0);

//    public ValueHolder(double max, double min, double mean) {
//        this.max = max;
//        this.min = min;
//        this.mean = mean;
//    }
    public ValueHolder(double max, double min, int sumForMean) {
        this.max = max;
        this.min = min;
        this.sumForMean = sumForMean;
    }

    public ValueHolder(double max, double min,AtomicInteger count, int sum) {
        this.sumForMean = sum;
        this.countAsync = count;
        this.max = max;
        this.min = min;
    }

    public int increaseCount() {
        return countAsync.getAndIncrement();
    }

    public int getCountAsync() {
        return this.countAsync.get();
    }

}
