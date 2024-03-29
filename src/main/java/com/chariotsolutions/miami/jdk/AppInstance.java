package com.chariotsolutions.miami.jdk;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds the stats for a particular app instance
 */
public class AppInstance implements Serializable {
    private Map<Integer, LatencyStats> latency;
    private Map<Integer, CapacityStats> capacity;
    private Map<Integer, CustomStats> custom;
    private long lastUpdateNanos;
    private long configuredNanos;
    private int lastSequenceNumber;

    //private static RiakRepository repo = null;

    //private static MongoRepository repo = null;
    private static RDBRepository repo = null;
    private static int maxRetries = 3;

    public AppInstance() {
        try {
            if(repo == null) {
                //repo = new RiakRepository();
                //repo = new MongoRepository();
                repo = new RDBRepository();
            }
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void update(long nanosSinceEpoch, int sequenceNumber) {
        this.lastUpdateNanos = nanosSinceEpoch;
        this.lastSequenceNumber = sequenceNumber;
    }

    public void reconfigure(long nanosSinceEpoch) {
        latency = new HashMap<Integer, LatencyStats>();
        capacity = new HashMap<Integer, CapacityStats>();
        custom = new HashMap<Integer, CustomStats>();
        configuredNanos = nanosSinceEpoch;
    }

    public void createLatencyStats(int index, LatencyStats stats) {
        latency.put(index, stats);
    }

    public LatencyStats getLatencyStats(int index) {
        if(latency == null) return null;
        return latency.get(index);
    }

    public void createCapacityStats(int index, CapacityStats stats) {
        capacity.put(index, stats);
    }

    public CapacityStats getCapacityStats(int index) {
        if(capacity == null) return null;
        return capacity.get(index);
    }

    public void createCustomStats(int index, CustomStats stats) {
        custom.put(index, stats);
    }

    public CustomStats getCustomStats(int index) {
        if(custom == null) return null;
        return custom.get(index);
    }

    public long getLastUpdateNanos() {
        return lastUpdateNanos;
    }

    public void storeStats(CapacityDataMessage cdm, int appId) {
        Date before = new Date();
        Map<String, Integer> statsMap = cdm.getStatsMap();

        StatsMeta meta = new StatsMeta(appId);
        StatStorageItem item = new StatStorageItem(cdm.getStats().getName(), "capacity", "MEI", meta.cloudId, meta.mpId, meta.appId, new Date().getTime());

        for(Map.Entry<String, Integer> entry : statsMap.entrySet()) {
            item.addStat(entry.getKey(), entry.getValue());
        }

        String storageKey = null;

       // int tries = 0;
        Exception lastException = null;
       // for(int i = 0; i < maxRetries; i++) {
       //     tries += 1;

            try {
                storageKey = repo.storeCurrentItem(item);
            } catch(Exception ex) {
               lastException = ex;
            }
        //    if(storageKey != null) break;
        //}

        Date after = new Date();
        long duration = after.getTime() - before.getTime();
        if(storageKey == null) {
            //System.err.println("failed to store item after " + duration + " msec. / " + tries + " tries: " + lastException.toString());
            System.err.println("failed to store item after " + duration + " msec. : " + lastException.toString());
        } else {
            //System.out.println("stored CapacityDataMessage with key: " + storageKey + " in " + duration + " msec. / " + tries + " tries");
            //System.out.println("stored CapacityDataMessage with key: " + storageKey + " in " + duration + " msec.");
        }
    }


    public void storeStats(LatencyDataMessage ldm, int appId) {
        Date before = new Date();
        Map<String, Integer> statsMap = ldm.getStatsMap();

        StatsMeta meta = new StatsMeta(appId);
        StatStorageItem item = new StatStorageItem(ldm.getStats().getName(), "latency", "MEI", meta.cloudId, meta.mpId, meta.appId, new Date().getTime());

        for(Map.Entry<String, Integer> entry : statsMap.entrySet()) {
            item.addStat(entry.getKey(), entry.getValue());
        }

        String storageKey = null;

        /*
        int tries = 0;
        Exception lastException = null;
        for(int i = 0; i < maxRetries; i++) {
            tries += 1;

            try {
                storageKey = repo.storeCurrentItem(item);
            } catch(Exception ex) {
               lastException = ex;
            }
            if(storageKey != null) break;
        }
        */
        Exception lastException = null;
        try {
            storageKey = repo.storeCurrentItem(item);
        } catch(Exception ex) {
            lastException = ex;
        }

        Date after = new Date();
        long duration = after.getTime() - before.getTime();
        if(storageKey == null) {
            //System.err.println("failed to store item after " + duration + " msec. / " + tries + " tries: " + lastException.toString());
            System.err.println("failed to store item after " + duration + " msec.: " + lastException.toString());
        } else {
            //System.out.println("stored LatencyDataMessage with key: " + storageKey + " in " + duration + " msec. / " + tries + " tries");
            //System.out.println("stored LatencyDataMessage with key: " + storageKey + " in " + duration + " msec.");
        }
    }

    public void storeStats(CustomDataMessage cdm, int appId) {
        Date before = new Date();
        Map<String, Integer> statsMap = cdm.getStatsMap();

        StatsMeta meta = new StatsMeta(appId);
        StatStorageItem item = new StatStorageItem(cdm.getStats().getName(), "custom", "MEI", meta.cloudId, meta.mpId, meta.appId, new Date().getTime());

        for(Map.Entry<String, Integer> entry : statsMap.entrySet()) {
            item.addStat(entry.getKey(), entry.getValue());
        }

        String storageKey = null;

        Exception lastException = null;
        try {
            storageKey = repo.storeCurrentItem(item);
        } catch(Exception ex) {
            lastException = ex;
        }

        Date after = new Date();
        long duration = after.getTime() - before.getTime();
        if(storageKey == null) {
            System.err.println("failed to store item after " + duration + " msec.: " + lastException.toString());
        } else {
           // System.out.println("stored CustomDataMessage with key: " + storageKey + " in " + duration + " msec.");
        }
    }

    class StatsMeta {
        public int cloudId;
        public int mpId;
        public int appId;

        public StatsMeta(int appId) {
           String appIdStr = "" + appId;
           if(appIdStr.length() == 7) {
               appIdStr = "0" + appIdStr;
           }
           this.cloudId = Integer.parseInt(appIdStr.substring(0, 2), 10);
           this.mpId = Integer.parseInt(appIdStr.substring(2, 4), 10);
           this.appId = Integer.parseInt(appIdStr.substring(4, 6), 10);
        }
    }

}
