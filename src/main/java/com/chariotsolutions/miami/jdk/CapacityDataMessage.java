package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads a message with capacity statistics
 */
public class CapacityDataMessage extends ChildMessage {
    private CapacityStats stats;
    public CapacityDataMessage(int sizeInBytes, byte type, byte item, ByteBuffer data, CapacityStats stats, double secondsElapsed) {
        super(sizeInBytes, type, item, data);
        this.stats = stats;
        stats.setTotalMessages(data.getInt());
        int count = data.getShort();
        if(count != stats.getValues().length) throw new IllegalStateException();
        for(int i=0;i <count; i++) {
            stats.setValue(i, data.getInt(), secondsElapsed);
        }
    }

    public CapacityStats getStats() {
        return stats;
    }


    public Map<String, Integer> getStatsMap() {
        Map<String, Integer> statsMap = new HashMap<String, Integer>();

        for (int i = 0; i < stats.getValues().length; i++) {
            String key = "CAP_" + (stats.getBucketSizeMillis() * (i + 1));
            Integer value = stats.getValues()[i];
            statsMap.put(key, value);
        }
        return statsMap;
    }
}
