package com.chariotsolutions.miami.simulator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A simulated measurement data packet
 */
public class DataPacket {
    private static final int PACKET_SIZE=390;
    private static byte[] PACKET_TEMPLATE = null;
    private ByteBuffer buffer;

    public DataPacket(int applicationID) throws IOException {
        buffer = ByteBuffer.allocateDirect(PACKET_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        initializePacketTemplate(applicationID);
        buffer.put(PACKET_TEMPLATE);
        buffer.putInt(2, applicationID);
    }

    private synchronized void initializePacketTemplate(int applicationID) throws IOException {
        if(PACKET_TEMPLATE == null) {
            InputStream in = getClass().getResourceAsStream("/data_packet_1.bin");
            if(in == null) {
                throw new RuntimeException("failed to load data_packet_1.bin for applicationID " + applicationID);
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

    public void addCustom1(int totalQuotesReq, int totalQuoteRejects, int totalQuoteRecords,
                           short totalMassCancelMpidReq, short totalMassCancelMpidUndReq, short totalMassCancelRej,
                           short totalEQuotesReq, short totalEQuotesRej, short totalArmResetReq,
                           short totalArmResetRej, short totalArmSettingsReq, short totalArmSettingsRej,
                           short totalLiquidityNotif, short totalArmTriggerNotif, short totalArmProtectionSettingsNotif,
                           short totalQuoteWidthReliefNotif, short totalTradingStatusNotif, short totalSystemStateNotif,
                           int totalCancelNotif, int totalExecutionNotif, int totalSeriesUpdate,
                           byte totalLineDisconnects, byte totalConnectRejects) {
        int offset = 14 // UDP Header
                    +62 // First Latency
                    +62 // Second Latency
                    +58 // Third Latency
                    +90 // Capacity
                    +4; // Message header
        addInt(offset, totalQuotesReq);
        addInt(offset+4, totalQuoteRejects);
        addInt(offset+8, totalQuoteRecords);
        addShort(offset+12, totalMassCancelMpidReq);
        addShort(offset+14, totalMassCancelMpidUndReq);
        addShort(offset+16, totalMassCancelRej);
        addShort(offset+18, totalEQuotesReq);
        addShort(offset+20, totalEQuotesRej);
        addShort(offset+22, totalArmResetReq);
        addShort(offset+24, totalArmResetRej);
        addShort(offset+26, totalArmSettingsReq);
        addShort(offset+28, totalArmSettingsRej);
        addShort(offset+30, totalLiquidityNotif);
        addShort(offset+32, totalArmTriggerNotif);
        addShort(offset+34, totalArmProtectionSettingsNotif);
        addShort(offset+36, totalQuoteWidthReliefNotif);
        addShort(offset+38, totalTradingStatusNotif);
        addShort(offset+40, totalSystemStateNotif);
        addInt(offset+42, totalCancelNotif);
        addInt(offset+46, totalExecutionNotif);
        addInt(offset+50, totalSeriesUpdate);
        addByte(offset+54, totalLineDisconnects);
        addByte(offset+55, totalConnectRejects);
    }

    public void addCustom2(int nTxUnseqMsgs, long nTxUnseqBytes, int nTxSeqMsgs, long nTxSeqBytes,
                           int nTxFails, int nRxUnseqMsgs, long nRxUnseqBytes) {
        int offset = 14 // UDP Header
                    +62 // First Latency
                    +62 // Second Latency
                    +58 // Third Latency
                    +90 // Capacity
                    +60 // Custom 1
                    +4; // Message header
        addInt(offset, nTxUnseqMsgs);
        addLong(offset+4, nTxUnseqBytes);
        addInt(offset+12, nTxSeqMsgs);
        addLong(offset+16, nTxSeqBytes);
        addInt(offset+24, nTxFails);
        addInt(offset+28, nRxUnseqMsgs);
        addLong(offset+32, nRxUnseqBytes);
    }

    /**
     * Sets the capacity values provided
     * @param values An array of 20 capacity values
     */
    public void setCapacityData(int[] values) {
        if(values.length != 20) throw new IllegalArgumentException("Got "+values.length+" capacity values but expecting 20!");
        int offset = 14 // UDP Header
                    +62 // First Latency
                    +62 // Second Latency
                    +58 // Third Latency
                    +10;// Header and first two fields
        for(int i=0;i<20; i++) {
            buffer.putInt(offset+i*4, values[i]);
        }
    }

    /**
     * Adds the latency values provided to the latency values
     * in the current data set.
     * @param index    Which latency data set (0-2)
     * @param newData  The new latency values (in ns) since the last call.  This method will calculate what buckets
     *                 to put them into
     */
    public void addLatencyData(int index, long[] newData) {
        if(newData.length == 0) return;
        int offset;
        long[] buckets;
        switch (index) {
            case 0:
                offset = 14+4; // UDP and message headers
                buckets = new long[]{199, 399, 599, 799, 1799, Long.MAX_VALUE};
                break;
            case 1:
                offset = 14+62+4; // headers plus first latency message
                buckets = new long[]{199, 399, 599, 799, 1799, Long.MAX_VALUE};
                break;
            case 2:
                offset = 14+62+62+4; // headers plus first two latency messages
                buckets = new long[]{999, 1999, 2999, 3999, Long.MAX_VALUE};
                break;
            default:
                throw new IllegalArgumentException("Latency index must be 0-2 (not "+index+")");
        }
        long min=newData[0], max=newData[0], total=0;
        int[] counts = new int[buckets.length];
        for (long l : newData) {
            if(l<min) min = l;
            if(l>max) max = l;
            total += l;
            for(int i=0; i<buckets.length; i++) {
                if(l < buckets[i]) {
                    ++counts[i];
                    break;
                }
            }
        }
        long average = total/newData.length;
        long working = 0;
        for (long l : newData) {
            long temp = (l-average)*(l-average);
            working += temp;
        }
        working = working / newData.length;
        long stddev = Math.round(Math.sqrt(working));

        buffer.putLong(offset, min);
        buffer.putLong(offset+8, max);
        buffer.putLong(offset+16, average);
        buffer.putLong(offset+24, stddev);
        int old;
        for(int i=0; i<counts.length; i++) {
            old = buffer.getInt(offset+34+i*4);
            buffer.putInt(offset+34+i*4, old+counts[i]);
        }
    }

    private void addLong(int offset, long value) {
        long temp = buffer.getLong(offset);
        buffer.putLong(offset, temp+value);
    }

    private void addInt(int offset, int value) {
        int temp = buffer.getInt(offset);
        buffer.putInt(offset, temp + value);
    }

    private void addShort(int offset, short value) {
        short temp = buffer.getShort(offset);
        buffer.putShort(offset, (short)(temp+value));
    }

    private void addByte(int offset, byte value) {
        byte temp = buffer.get(offset);
        buffer.put(offset, (byte)(temp+value));
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getSize() {return PACKET_SIZE;}

    public static void main(String[] args) throws IOException {
        DataPacket dp = new DataPacket(123456);
        dp.prepare((short)22);
        dp.addLatencyData(0, new long[]{40, 5245, 3432, 45, 99, 245, 288, 536, 232});
        dp.addLatencyData(1, new long[]{32, 45, 894, 345, 73, 345, 234, 456, 122, 34});
        dp.addLatencyData(2, new long[]{234, 67, 93, 843, 123, 258, 394, 234, 347, 373, 4023, 3203, 2943});
        dp.setCapacityData(new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20});
        dp.addCustom1(5, 3, 4, (short) 5, (short) 9, (short) 8, (short) 3, (short) 3, (short) 2, (short) 7,
                (short) 1, (short) 6, (short) 3, (short) 8, (short) 9, (short) 2, (short) 4, (short) 8,
                2, 23, 14, (byte) 1, (byte) 2);
        dp.addCustom2(3, 487, 89, 502456, 1, 14, 234324);
        ByteBuffer bb = dp.getBuffer();
        FileOutputStream out = new FileOutputStream("test_packet.bin");
        byte[] foo = new byte[bb.limit()];
        bb.rewind();
        bb.get(foo);
        out.write(foo);
        out.flush();
        out.close();
    }
}
