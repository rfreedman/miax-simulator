package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;

/**
 * A message that configures future custom stats
 */
public class CustomConfigMessage extends ChildMessage {
    private final CustomStats stats;

    public CustomConfigMessage(int sizeInBytes, byte type, byte item, ByteBuffer data) {
        super(sizeInBytes, type, item, data);
        StringBuilder nb = new StringBuilder();
        for(int i=0; i<STRING_LENGTH; i++) {
            char c = (char)data.get();
            if(c > 0) nb.append(c);
        }
        int count = data.getShort();
        stats = new CustomStats();
        stats.initialize(nb.toString().trim(), count);
        for(int i=0; i<count; i++) {
            nb.setLength(0);
            for(int j=0; j<STRING_LENGTH; j++) {
                char c = (char)data.get();
                if(c > 0) nb.append(c);
            }
            int fieldType = data.getInt();
            int fieldLength;
            switch (fieldType) {
                case 0:
                case 1:
                    fieldLength = 1;
                    break;
                case 2:
                case 3:
                    fieldLength = 2;
                    break;
                case 4:
                case 5:
                    fieldLength = 4;
                    break;
                case 6:
                case 7:
                    fieldLength = 8;
                    break;
                default:
                    throw new IllegalStateException("Unexpected custom field type '"+fieldType+"'");
            }
            stats.setField(i, nb.toString().trim(), fieldLength);
        }
    }

    public CustomStats getStats() {
        return stats;
    }
}
