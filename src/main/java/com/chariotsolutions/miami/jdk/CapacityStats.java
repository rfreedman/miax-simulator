package com.chariotsolutions.miami.jdk;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds latency statistics
 */
public class CapacityStats {
    private String name;
    private int totalMessages;
    private int bucketSizeMillis;
    private int[] values;

    public void initialize(String name, int count, int bucketSizeMillis) {
        this.name = name;
        values = new int[count];
        this.bucketSizeMillis = bucketSizeMillis;
    }

    public void setValue(int bucket, int value, double secondsElapsed) {
        values[bucket] = value;
    }

    public void setTotalMessages(int totalMessages) {
        this.totalMessages = totalMessages;
    }

    public String getName() {
        return name;
    }

    public int[] getValues() {
        return values;
    }

    public int getTotalMessages() {
        return totalMessages;
    }

    public int getBucketSizeMillis() {
        return bucketSizeMillis;
    }

    public void print() {
            System.out.println("CAPACITY "+name);
            System.out.print("|");
            int[] sizes = new int[values.length];
            for(int i=0; i<values.length;i++) {
                String s = (bucketSizeMillis*(i+1)) + "|";
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
    }
}
