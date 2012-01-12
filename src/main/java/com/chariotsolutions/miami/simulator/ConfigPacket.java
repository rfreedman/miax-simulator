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
    private static byte[] PACKET_TEMPLATE = null;
    private ByteBuffer buffer;

    public ConfigPacket(int applicationID) throws IOException {
        buffer = ByteBuffer.allocateDirect(PACKET_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        initializePacketTemplate(applicationID);
        buffer.put(PACKET_TEMPLATE);
        buffer.putInt(2, applicationID);
    }

    private synchronized void initializePacketTemplate(int applicationID) throws IOException {
        if(PACKET_TEMPLATE == null) {
            InputStream in = getClass().getResourceAsStream("/config_packet.bin");
            if(in == null) {
                throw new RuntimeException("failed to load config_packet.bin for applicationID " + applicationID);
            }

            PACKET_TEMPLATE = new byte[PACKET_SIZE];
            if(in.read(PACKET_TEMPLATE, 0, PACKET_SIZE) != PACKET_SIZE) throw new IllegalStateException("Expecting to read whole buffer in one go!");
            in.close();
        }

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
       System.out.println( new ConfigPacket(43562).getBuffer());
    }
}
