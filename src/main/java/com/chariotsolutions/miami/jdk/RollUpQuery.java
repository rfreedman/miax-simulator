package com.chariotsolutions.miami.jdk;

import java.util.*;

// rolls up data for one app type across all clouds/instances/mpids,
// providing one 'row' of stats as a hashmap - this is absolute worst-case
public class RollUpQuery {
/*
    private static RiakRepository repo;

    public RollUpQuery() {
       try {
            if(repo == null) {
                repo = new RiakRepository();
            }
       } catch (Exception ex) {
           ex.printStackTrace();
       }
    }

    public Map<String, Integer> rollUpAppTypeStats(String appType) throws Exception {
        Map<String, Integer> rollUp = new HashMap<String, Integer>();

        long before = new Date().getTime();
        Collection<StatStorageItem> items = repo.getCurrentItems("MEI", null, null, null);
        long after = new Date().getTime();
        long duration = after - before;
        System.out.println("retrieved " + items.size() + " items in " + duration + " msec.");

        before = new Date().getTime();

        for(StatStorageItem item : items) {
            for(Map.Entry<String, Integer> entry : item.getStats().entrySet()) {
                if(rollUp.containsKey(entry.getKey())) {
                    rollUp.put(entry.getKey(), rollUp.get(entry.getKey()) + entry.getValue());
                } else {
                    rollUp.put(entry.getKey(), entry.getValue());
                }
            }
        }

        after = new Date().getTime();
        duration = after - before;
        System.out.println("rollup took " + duration + " msec.");
        return rollUp;
    }

    public Map<Integer, Map<String, Integer>> rollUpAppTypeStatsByCloud(String appType) throws Exception {
        
        Map<Integer, Map<String, Integer>> cloudRollups = new TreeMap<Integer, Map<String, Integer>>(
                new Comparator<Integer>() {
                    public int compare(Integer integer, Integer integer1) {
                        return integer.compareTo(integer1);
                    }
                }
        );

        long before = new Date().getTime();
        //Collection<StatStorageItem> items = repo.getCurrentItems("MEI", null, null, null);
        Collection<StatStorageItem> items = repo.getCurrentItemsMultiThreaded("MEI", null, null, null);
        long after = new Date().getTime();
        long duration = after - before;
        System.out.println("retrieved " + items.size() + " items in " + duration + " msec.");

        before = new Date().getTime();

        for(StatStorageItem item : items) {
            // find the rollup for this item's cloud
            int cloudId = item.getCloudId();
            Map<String, Integer> rollUp = cloudRollups.get(cloudId);
            if(rollUp == null) {
                rollUp = new HashMap<String, Integer>();
                cloudRollups.put(cloudId, rollUp);
            }

            for(Map.Entry<String, Integer> entry : item.getStats().entrySet()) {
                if(rollUp.containsKey(entry.getKey())) {
                    rollUp.put(entry.getKey(), rollUp.get(entry.getKey()) + entry.getValue());
                } else {
                    rollUp.put(entry.getKey(), entry.getValue());
                }
            }

        }

        after = new Date().getTime();
        duration = after - before;
        System.out.println("rollup took " + duration + " msec.");

        return cloudRollups;
    }
    */
}
