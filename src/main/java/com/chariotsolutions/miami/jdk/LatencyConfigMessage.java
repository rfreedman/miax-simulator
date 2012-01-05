package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;

/**
 * A message that configures future latency stats
 */
public class LatencyConfigMessage extends ChildMessage {
    private final LatencyStats stats;

    public LatencyConfigMessage(int sizeInBytes, byte type, byte item, ByteBuffer data) {
        super(sizeInBytes, type, item, data);
        StringBuilder nb = new StringBuilder();
        for(int i=0; i<STRING_LENGTH; i++) {
            char c = (char)data.get();
            if(c > 0) nb.append(c);
        }
        int count = data.getShort();
        stats = new LatencyStats();
        stats.initialize(nb.toString().trim(), count);
        for(int i=0; i<count; i++) {
            int min = data.getInt();
            int max = data.getInt();
            stats.setRange(i, min, max);
        }
    }

    public LatencyStats getStats() {
        return stats;
    }
}
