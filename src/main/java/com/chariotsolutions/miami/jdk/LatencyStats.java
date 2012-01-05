package com.chariotsolutions.miami.jdk;

/**
 * Holds latency statistics
 */
public class LatencyStats {
    private String name;
    private int[] min;
    private int[] max;
    private int[] values;
    private long intervalMin;
    private long intervalMax;
    private long intervalAverage;
    private long intervalStandardDeviation;
    private double[] changePerSecond;

    public void initialize(String name, int count) {
        this.name = name;
        min = new int[count];
        max = new int[count];
        values = new int[count];
        changePerSecond = new double[count];
    }

    public void setRange(int bucket, int min, int max) {
        this.min[bucket] = min;
        this.max[bucket] = max;
    }

    public void setValue(int bucket, int value, double secondsElapsed) {
        changePerSecond[bucket] = (value - values[bucket])/secondsElapsed;
        values[bucket] = value;
    }

    public void setIntervalStatistics(long intervalMin, long intervalMax, long intervalAverage, long intervalStandardDeviation) {
        this.intervalMin = intervalMin;
        this.intervalMax = intervalMax;
        this.intervalAverage = intervalAverage;
        this.intervalStandardDeviation = intervalStandardDeviation;
    }

    public String getName() {
        return name;
    }

    public int[] getMin() {
        return min;
    }

    public int[] getMax() {
        return max;
    }

    public int[] getValues() {
        return values;
    }

    public double[] getChangePerSecond() {
        return changePerSecond;
    }

    public long getIntervalMin() {
        return intervalMin;
    }

    public long getIntervalMax() {
        return intervalMax;
    }

    public long getIntervalAverage() {
        return intervalAverage;
    }

    public long getIntervalStandardDeviation() {
        return intervalStandardDeviation;
    }

    public void print() {
            System.out.println("LATENCY "+name);
            System.out.print("|");
            int[] sizes = new int[values.length];
            for(int i=0; i<values.length;i++) {
                String s = min[i] + "-" + max[i] + "|";
                sizes[i] = s.length()-1;
                System.out.print(s);
            }
            System.out.println();
            System.out.print("|");
            for(int i=0; i<values.length; i++) {
                String s = "  "+values[i];
                for(int j=s.length();j<sizes[i];j++) s+=" ";
                System.out.print(s + "|");
            }
            System.out.println();
            System.out.print("D");
            for(int i=0; i<values.length; i++) {
                String s = changePerSecond[i]+"              ";
                s = s.substring(0, sizes[i]);
                System.out.print(s + "|");
            }
            System.out.println();
            System.out.println("Interval:  Min="+intervalMin+", max="+intervalMax+", ave="+intervalAverage+", dev="+intervalStandardDeviation);
    }
}
