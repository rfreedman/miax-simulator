package com.chariotsolutions.miami.simulator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A simulated configuration packet
 */
public class ConfigPacket {
    private static final int PACKET_SIZE=2612;
    private ByteBuffer buffer;

    public ConfigPacket(int applicationID) throws IOException {
        buffer = ByteBuffer.allocateDirect(PACKET_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        InputStream in = getClass().getResourceAsStream("/config_packet.bin");
        byte[] temp = new byte[PACKET_SIZE];
        if(in.read(temp, 0, PACKET_SIZE) != PACKET_SIZE) throw new IllegalStateException("Expecting to read whole buffer in one go!");
        in.close();
        buffer.put(temp);
        buffer.putInt(2, applicationID);
    }

    public void prepare(short sequenceNum) {
        buffer.putShort(0, sequenceNum);
        buffer.putLong(6, System.currentTimeMillis()*1000);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getSize() {return PACKET_SIZE;}

    public static void main(String[] args) throws IOException {
        new ConfigPacket(43562).getBuffer();
    }
}
