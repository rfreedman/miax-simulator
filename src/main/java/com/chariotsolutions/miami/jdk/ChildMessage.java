package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;

/**
 * Base class for the child messages within a stats message
 */
public class ChildMessage {
    public final static int STRING_LENGTH=64;
    private int sizeInBytes;
    private byte type;
    private byte item;
    protected ByteBuffer data;

    public ChildMessage(int sizeInBytes, byte type, byte item, ByteBuffer data) {
        this.sizeInBytes = sizeInBytes;
        this.type = type;
        this.item = item;
        this.data = data;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public byte getType() {
        return type;
    }

    public byte getItem() {
        return item;
    }
}
