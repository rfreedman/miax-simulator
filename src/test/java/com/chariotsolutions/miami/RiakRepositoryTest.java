package com.chariotsolutions.miami;


import com.chariotsolutions.miami.jdk.RiakRepository;
import com.chariotsolutions.miami.jdk.RollUpQuery;
import com.chariotsolutions.miami.jdk.StatStorageItem;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class RiakRepositoryTest {

    /*
    @Test
    public void testDeleteCurrentStats() throws Exception {

        // wait for all of the nodes to catch up in case we just ran this
        //Thread.sleep(10000L);

        RiakRepository repo = new RiakRepository();
        Date before = new Date();
        int itemsDeleted = repo.deleteCurrentStats();
        Date after = new Date();
        long duration = after.getTime() - before.getTime();
        System.out.println("deleted " + itemsDeleted + " items in " + duration + " msec.");


        // wait for all of the nodes to catch up...
        Thread.sleep(10000L);
        
        itemsDeleted = repo.deleteCurrentStats();
        assertEquals("unexpected number of items deleted", 0, itemsDeleted);


        StatStorageItem item = new StatStorageItem("MEI", 1, 2, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
        repo.storeCurrentItem(item);

        itemsDeleted = repo.deleteCurrentStats();
        assertEquals("unexpected number of items deleted", 1, itemsDeleted);



        System.out.println("storing 25,000 keys");
        for(int i = 0; i < 25000; i++) {
             item = new StatStorageItem("MEI", i, 2, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
            repo.storeCurrentItem(item);
        }

        System.out.println("deleting 25,000 keys");
        before = new Date();
        itemsDeleted = repo.deleteCurrentStats();
        after = new Date();
        duration = after.getTime() - before.getTime();
        System.out.println("deleted " + itemsDeleted + " items in " + duration + " msec.");
        assertEquals("unexpected number of items deleted", 25000, itemsDeleted);

    }


    @Test
    public void testStoreLotsOfItems() throws Exception {
        System.out.println("storing 1,000 items");
        StatStorageItem item = null;
        RiakRepository repo = new RiakRepository();
        Date before = new Date();
        for(int i = 0; i < 1000; i++) {
            item = new StatStorageItem("MEI", i, 2, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
           repo.storeCurrentItem(item);
        }
        Date after = new Date();
        long duration = after.getTime() - before.getTime();
        System.out.println("stored 1,000 items in " + duration + "msec");
        Thread.sleep(3000);
        repo.deleteCurrentStats();
    }

    @Test
    public void testStoreAndRetrieveByKey() throws Exception {
        RiakRepository repo = new RiakRepository();

        StatStorageItem item = new StatStorageItem("MEI", 1, 2, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
        String key = repo.storeCurrentItem(item);
        assertNotNull("null key", key);

        StatStorageItem item2 = repo.getCurrentItemByKey(key);
        assertEquals("unexpected foo value", new Integer(100), item2.getStats().get("foo"));
        assertEquals("unexpected foo value", new Integer(365), item2.getStats().get("bar"));
        assertEquals("unexpected timestamp", item.getTimestamp(), item2.getTimestamp());

        // clean up
        repo.deleteCurrentStats();
    }

    @Test
    public void testRetrieveByCompoundKey() throws Exception {

        RiakRepository repo = new RiakRepository();

        repo.deleteCurrentStats();
        Thread.sleep(3000);

        StatStorageItem item1 = new StatStorageItem("MEI", 1, 2, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);

        repo.storeCurrentItem(item1);

        StatStorageItem item2 = new StatStorageItem("MEI", 1, 2, 4, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
        repo.storeCurrentItem(item2);

        StatStorageItem item3 = new StatStorageItem("MEI", 1, 5, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
        repo.storeCurrentItem(item3);

        StatStorageItem item4 = new StatStorageItem("MEI", 2, 5, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);
        repo.storeCurrentItem(item4);

        // all MEI stats in cloud 1
        long before = new Date().getTime();
        Collection<StatStorageItem> items = repo.getCurrentItems("MEI", 1, null, null);
        long after = new Date().getTime();
        long duration = after - before;
        assertNotNull("got no items", items);
        assertEquals("got unexpected number of items", 3, items.size());
        System.out.println("got 3 items for MEI / cloud 1 in " + duration + " msec.");

        // all MEI stats in cloud 1 for mpid (firm) 2
        items = repo.getCurrentItems("MEI", 1, 2, null);
        assertNotNull("got no items", items);
        assertEquals("got unexpected number of items", 2, items.size());

        // all MEI stats in cloud 1 for app id 3
        items = repo.getCurrentItems("MEI", 1, null, 3);
        assertNotNull("got no items", items);
        assertEquals("got unexpected number of items", 2, items.size());


        // all MEI stats in all clouds for app id 3
        items = repo.getCurrentItems("MEI", null, null, 3);
        assertNotNull("got no items", items);
        assertEquals("got unexpected number of items", 3, items.size());


        // all MEI stats
        items = repo.getCurrentItems("MEI", null, null, null);
        assertNotNull("got no items", items);
        assertEquals("got unexpected number of items", 4, items.size());

        repo.deleteCurrentStats();
        Thread.sleep(3000);
    }


    @Test
    public void testRetreiveLotsOfStats() throws Exception {

        final int ITEM_COUNT = 1536;

        RiakRepository repo = new RiakRepository();

        System.out.println("deleting all stats in prep for test");
        repo.deleteCurrentStats();

        System.out.println("waiting 3 seconds for Riak to quiesce");
        Thread.sleep(3000);

        System.out.println("storing " + ITEM_COUNT + " items");
        for(int i = 0; i < ITEM_COUNT; i++) {
             StatStorageItem item = new StatStorageItem("MEI", 1, 2, i, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365);

             repo.storeCurrentItem(item);
        }

        for(int rep = 0; rep < 20; rep++) {
            System.out.print("retrieving " + ITEM_COUNT + " items...");
            long before = new Date().getTime();
            Collection<StatStorageItem> items = repo.getCurrentItems("MEI", 1, null, null);
            long after = new Date().getTime();
            long duration = after - before;
            System.out.println("retrieved 500 items in " + duration + " msec.");
        }
        
        System.out.println("cleaning up....");
        repo.deleteCurrentStats();
        Thread.sleep(3000);
    }

    @Test
    public void testAppTypeRollup() throws Exception {
         final int ITEM_COUNT = 1536;

        RiakRepository repo = new RiakRepository();

        System.out.println("deleting all stats in prep for test");
        repo.deleteCurrentStats();

        System.out.println("waiting 3 seconds for Riak to quiesce");
        Thread.sleep(3000);

        System.out.println("storing " + ITEM_COUNT + " items");
        for(int i = 0; i < ITEM_COUNT; i++) {
             StatStorageItem item = new StatStorageItem("MEI", 1, 2, i, new Date().getTime());
             for(int j = 0; j < 100; j++) {
                item.addStat("stat" + j, j);
             }
             repo.storeCurrentItem(item);
        }

        RollUpQuery query = new RollUpQuery();
        Map<String, Integer> rollUp =  query.rollUpAppTypeStats("MEI");

        for(String key : rollUp.keySet()) {
            System.out.println(key + ": " + rollUp.get(key));
        }
    }

    @Test
    public void testAppTypeRollupByCloud() throws Exception {
        final int cloudCount = 24;
        final int instancesPerCloud = 32;

        RiakRepository repo = new RiakRepository();


        System.out.println("deleting all stats in prep for test");
        repo.deleteCurrentStats();

        System.out.println("waiting 3 seconds for Riak to quiesce");
        Thread.sleep(3000);

        System.out.println("storing " + (cloudCount * instancesPerCloud) + " items (" + cloudCount + " clouds , " +  instancesPerCloud  + " instances each)");

        for(int cloud = 1; cloud <= cloudCount; cloud++) {
             for(int i = 0; i < instancesPerCloud; i++) {
                 StatStorageItem item = new StatStorageItem("MEI", cloud, 2, i, new Date().getTime());
                 for(int j = 0; j < 100; j++) {
                    item.addStat("stat" + j, j);
                 }
                 repo.storeCurrentItem(item);
            }
        }

        RollUpQuery query = new RollUpQuery();
        Map<Integer, Map<String, Integer>> rollUp = null;

        for(int k = 0; k < 10; k ++) {
            rollUp =  query.rollUpAppTypeStatsByCloud("MEI");
        }
        

        TestTimer timer = new TestTimer("rollup appType stats by cloud:").start();
        rollUp =  query.rollUpAppTypeStatsByCloud("MEI");
        timer.stop();

        for(Integer key : rollUp.keySet()) {
            System.out.println(key + ": " + rollUp.get(key));
        }

        System.out.println(timer);
    }

    @Test
    public void testConcurrentQueries() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(20);
        Set<Future<TestTimer>> futures = new HashSet<Future<TestTimer>>();
        for(int i = 0; i < 2; i++) {
            Callable<TestTimer> callable = new QueryTestExecutor(i);
            Future<TestTimer> future = pool.submit(callable);
            futures.add(future);
            System.out.println("submitted query #" + i);
        }

        for(Future<TestTimer> future : futures) {
            System.out.println(future.get());
        }
    }


    @Test
    public void testContinuousQuery() throws Exception {
        TestTimer timer = new TestTimer("test continuous query:");
        while(true) {
            try {
                timer.start();
                RollUpQuery query = new RollUpQuery();
                Map<Integer, Map<String, Integer>> rollUp = query.rollUpAppTypeStatsByCloud("MEI");
                timer.stop();
                for(Integer key : rollUp.keySet()) {
                    System.out.println(key + ": " + rollUp.get(key));
                }
                System.out.println(timer);
            } catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public class QueryTestExecutor implements Callable<TestTimer> {
        private int index = 0;
        public QueryTestExecutor(int index) {
            this.index = index;
        }
        
        public TestTimer call() throws Exception {
            TestTimer timer = new TestTimer("test concurrent query: " + index).start();
            RollUpQuery query = new RollUpQuery();
            query.rollUpAppTypeStatsByCloud("MEI");
            timer.stop();
            return timer;
        }
    }

    public class TestTimer{
        private String description;
        private long before;
        private long after;

        public TestTimer() {

        }

        public TestTimer(String description) {
            this.description = description;
        }

        public TestTimer start() {
            this.before = new Date().getTime();
            this.after = 0;
            return this;
        }

        public TestTimer stop() {
            this.after = new Date().getTime();
            return this;
        }

        public long getDuration() {
            return after - before;
        }

        public String toString() {
            return description + " " + getDuration() + " msec.";
        }
    }
    */
}
