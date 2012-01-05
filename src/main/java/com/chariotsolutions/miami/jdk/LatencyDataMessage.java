package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;

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
}
