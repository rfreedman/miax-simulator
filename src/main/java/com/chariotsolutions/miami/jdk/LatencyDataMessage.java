package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads a message with latency statistics
 */
public class LatencyDataMessage extends ChildMessage {
    private LatencyStats stats;
    public LatencyDataMessage(int sizeInBytes, byte type, byte item, ByteBuffer data, LatencyStats stats, double secondsElapsed) {
        super(sizeInBytes, type, item, data);
        this.stats = stats;
        stats.setIntervalStatistics(data.getLong(),data.getLong(),data.getLong(),data.getLong());
        int count = data.getShort();
        if(count != stats.getValues().length) throw new IllegalStateException();
        for(int i=0;i <count; i++) {
            stats.setValue(i, data.getInt(), secondsElapsed);
        }
    }

    public LatencyStats getStats() {
        return stats;
    }

    public Map<String, Integer> getStatsMap() {
        Map<String, Integer> statsMap = new HashMap<String, Integer>();

        for (int i = 0; i < stats.getValues().length; i++) {
            String key = getStatKey(i);
            Integer value = stats.getValues()[i];
            statsMap.put(key, value);
        }
        return statsMap;
    }

    private String getStatKey(int index) {
        String minName = "" + stats.getMin()[index];
        String maxName;
        if(stats.getMax()[index] == -1) {
            maxName = "INF";
        } else {
            maxName = "" + stats.getMax()[index];
        }

        return "LAT_" + minName + "_" + maxName;
    }
}
