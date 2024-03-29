package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads a message with custom statistics
 */
public class CustomDataMessage extends ChildMessage {
    private CustomStats stats;

    public CustomDataMessage(int sizeInBytes, byte type, byte item, ByteBuffer data, CustomStats stats, double secondsElapsed) {
        super(sizeInBytes, type, item, data);
        this.stats = stats;
        for(int i=0;i <stats.getValues().length; i++) {
            stats.readValue(i, data, secondsElapsed);
        }
        if(data.limit() != data.position()) {
            throw new IllegalStateException("Did not read to end of custom data ("+data.position()+"/"+data.limit()+")");
        }
    }

    public CustomStats getStats() {
        return stats;
    }

    public Map<String, Integer> getStatsMap() {
        Map<String, Integer> statsMap = new HashMap<String, Integer>();

        for (int i = 0; i < stats.getValues().length; i++) {
            String key = stats.getFields()[i].name;
            Integer value = stats.getValues()[i].intValue();
            statsMap.put(key, value);
        }
        return statsMap;
    }
}
