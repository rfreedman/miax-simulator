package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;

/**
 * A message that configures future capacity stats
 */
public class CapacityConfigMessage extends ChildMessage {
    private final CapacityStats stats;

    public CapacityConfigMessage(int sizeInBytes, byte type, byte item, ByteBuffer data) {
        super(sizeInBytes, type, item, data);
        StringBuilder nb = new StringBuilder();
        for(int i=0; i<STRING_LENGTH; i++) {
            char c = (char)data.get();
            if(c > 0) nb.append(c);
        }
        int millis = data.getShort();
        int count = data.getShort();
        stats = new CapacityStats();
        stats.initialize(nb.toString().trim(), count, millis);
    }

    public CapacityStats getStats() {
        return stats;
    }
}
