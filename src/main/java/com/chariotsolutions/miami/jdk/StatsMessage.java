package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

/**
 * A message in the stats protocol
 */
public class StatsMessage implements Runnable {
    private int sequence;
    private int applicationId;
    private long nanosSinceEpoch;
    private ChildMessage[] children;
    private ByteBuffer networkData;
    private AppInstance instance;

    private byte[] msgBytes;

    public StatsMessage(byte[] bytes) {
        this.msgBytes = bytes;
    }

    public StatsMessage() {

    }

    private StatsMessage(int sequence, int applicationId, long nanosSinceEpoch, ByteBuffer networkData) {
        this.sequence = sequence;
        this.applicationId = applicationId;
        this.nanosSinceEpoch = nanosSinceEpoch;
        this.networkData = networkData;
    }

    public static StatsMessage readHeader(ByteBuffer buf) {
        int sequence = buf.getShort();
        int applicationId = buf.getInt();
        long nanos = buf.getLong();
        StatsMessage msg = new StatsMessage(sequence, applicationId, nanos, buf);
        return msg;
    }

    public void setAppInstance(AppInstance instance) {
        this.instance = instance;
    }

    public void run() {
        //synchronized(instance) {
        {
            if(msgBytes != null) {
                this.networkData = ByteBuffer.wrap(msgBytes);
                networkData.order(ByteOrder.LITTLE_ENDIAN);
                this.sequence = networkData.getShort();
                this.applicationId = networkData.getInt();
                this.nanosSinceEpoch = networkData.getLong();
                this.msgBytes = null;
            }

            double elapsedSeconds = (nanosSinceEpoch - instance.getLastUpdateNanos())/1000000d;
            instance.update(nanosSinceEpoch, sequence);
            ArrayList<ChildMessage> list = new ArrayList<ChildMessage>();
            boolean reconfigured = false;
            while(networkData.limit() - networkData.position() > 4) {
                int size = networkData.getShort();
                byte type = networkData.get();
                byte item = networkData.get();
                int totalPosition = networkData.position()+size-4;
                int totalLimit = networkData.limit();
                networkData.limit(totalPosition);
                // Assumes this will process the buffer data
                ChildMessage child = null;
                switch (type) {

                    case 1: // Latency Config Message
                        if(!reconfigured) {
                            instance.reconfigure(nanosSinceEpoch);
                            reconfigured = true;
                        }
                        LatencyConfigMessage lcm = new LatencyConfigMessage(size, type, item, networkData);
                        child = lcm;
                        instance.createLatencyStats(child.getItem(), lcm.getStats());
                        break;

                    case 2:   // Capacity Config Message
                        if(!reconfigured) {
                            instance.reconfigure(nanosSinceEpoch);
                            reconfigured = true;
                        }
                        CapacityConfigMessage ccm = new CapacityConfigMessage(size, type, item, networkData);
                        child = ccm;
                        instance.createCapacityStats(child.getItem(), ccm.getStats());
                        break;

                    case 3:   // Custom Config Message
                        if(!reconfigured) {
                            instance.reconfigure(nanosSinceEpoch);
                            reconfigured = true;
                        }
                        CustomConfigMessage mcm = new CustomConfigMessage(size, type, item, networkData);
                        child = mcm;
                        instance.createCustomStats(child.getItem(), mcm.getStats());
                        break;

                    case 4: // Latency Data
                        LatencyStats latencyStats = instance.getLatencyStats(item);
                        if(latencyStats != null) {
                            LatencyDataMessage ldm = new LatencyDataMessage(size, type, item, networkData, latencyStats, elapsedSeconds);
                            child = ldm;
                            instance.storeStats(ldm, applicationId);
                        } // else wait for a configuration packet
                        break;

                    case 5:  // Capacity Data
                        CapacityStats capacityStats = instance.getCapacityStats(item);
                        if(capacityStats != null) {
                            CapacityDataMessage cdm = new CapacityDataMessage(size, type, item, networkData, capacityStats, elapsedSeconds);
                            child = cdm;
                            //cdm.getStats().print();
                            instance.storeStats(cdm, applicationId);
                        } // else wait for a configuration packet
                        break;

                    case 6: // Custom Data
                        CustomStats customStats = instance.getCustomStats(item);
                        if(customStats != null) {
                            CustomDataMessage mdm = new CustomDataMessage(size, type, item, networkData, customStats, elapsedSeconds);
                            child = mdm;
                            instance.storeStats(mdm, applicationId);
                        } // else wait for a configuration packet
                        break;

                    default:
                        child = new ChildMessage(size, type, item, networkData);
                }
                networkData.position(totalPosition);
                networkData.limit(totalLimit);
                if(child != null) list.add(child);
            }
            children = list.toArray(new ChildMessage[list.size()]);
        }
    }

    public int getSequence() {
        return sequence;
    }

    public int getApplicationId() {
        return applicationId;
    }

    public long getNanosSinceEpoch() {
        return nanosSinceEpoch;
    }

    public ChildMessage[] getChildren() {
        return children;
    }
}
