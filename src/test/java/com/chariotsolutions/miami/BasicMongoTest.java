package com.chariotsolutions.miami;

import com.mongodb.*;
import com.sun.org.apache.xerces.internal.impl.dv.DatatypeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BasicMongoTest {
    private Mongo mongo = null;
    private DB db = null;

    private Mongo mongo2 = null;
    private DB db2 = null;

    @Before
    public void setup() throws Exception {
      mongo = new Mongo("127.0.0.1", 27017);
      db = mongo.getDB("storage-test");


      mongo2 = new Mongo("127.0.0.1", 27027);
      db2 = mongo2.getDB("storage-test");
    }


    @After
    public void teardown() {
        db.dropDatabase();
        db2.dropDatabase();
    }


    @Test
    public void multiTest() throws Exception {
        for(int i = 0; i < 1000; i++) {
            testMongoSerialStorageThroughput();
        }
    }

    @Test
    public void testMongoSerialStorageThroughput() throws Exception {
        int itemCount = 9000;
        DBCollection collection = db.getCollection("test-data");


        collection.ensureIndex(new BasicDBObject("key0", 1));
        collection.ensureIndex(new BasicDBObject("key1", 1));
        collection.ensureIndex(new BasicDBObject("key2", 1));
        collection.ensureIndex(new BasicDBObject("key3", 1));
        collection.ensureIndex(new BasicDBObject("key4", 1));
        collection.ensureIndex(new BasicDBObject("key5", 1));


        long before = new Date().getTime();
        saveABunch(collection, itemCount);
        long after = new Date().getTime();
        assertEquals("unexpected item count", itemCount, collection.getCount());
        printStats(itemCount, before, after, "insert");


        before = new Date().getTime();
        saveABunch(collection, itemCount);
        after = new Date().getTime();
        assertEquals("unexpected item count", itemCount, collection.getCount());
        printStats(itemCount, before, after, "update");
    }

    private void printStats(int itemCount, long before, long after, String description)  {
        System.out.println("time to " + description + " " +  itemCount + " items: " + (after - before) + " msec.");    // avg 1400 - 1450 msec.

        double dbefore = before * 1.0;
        double dafter = after * 1.0;
        double duration = after - before;
        double ditemcount = itemCount * 1.0;
        double avg = duration / itemCount;
        System.out.println("average time per item: " +  avg + " msec.");       // avg 0.25 msec   (0.10 msec without getLastError() )

    }

    private void saveABunch(DBCollection collection, int itemCount) {
        for(int i = 0; i < itemCount; i++) {
            BasicDBObject doc = new BasicDBObject();
            for(int j = 0; j < 60; j++) {
                doc.put("key" + j, "value" + j);
            }
            doc.put("_id", "id" + i);
            collection.save(doc);

            /*
            DBObject error  = db.getLastError();
            if(error.containsField("err") && error.get("err") != null) {
                //throw new Exception((String)error.get("err"));
                fail("Error: " + error.get("err"));
            }
            */

        }

    }

    @Test
    public void testMultithreaded() throws Exception {
        int batchSize = 9000;
        int batchCount = 1;

        // 900 per batch * 10 batches = 9000 : duration = ~400 msec.
        // 90 per batch * 1000 batches = 9000: duration = ~ 400 msec.
        // 9000 per bathc * 1 batch = ~ 400 msec.

        DBCollection collection = db.getCollection("test-data");


        collection.ensureIndex(new BasicDBObject("key0", 1));
        collection.ensureIndex(new BasicDBObject("key1", 1));
        collection.ensureIndex(new BasicDBObject("key2", 1));
        collection.ensureIndex(new BasicDBObject("key3", 1));
        collection.ensureIndex(new BasicDBObject("key4", 1));
        collection.ensureIndex(new BasicDBObject("key5", 1));

        for(int i = 0; i < 1000; i++) {
            long before = new Date().getTime();
            saveABunchMultiThreaded(collection, batchSize, batchCount);
            long after = new Date().getTime();
            System.out.println("time to save " + (batchCount * batchSize) + " items in " + batchCount + " batches of " + batchSize + ": " + (after - before) + " msec.");
        }
    }

    private void saveABunchMultiThreaded(DBCollection collection, int batchSize, int batchCount) throws Exception {

        ExecutorService threadPool = Executors.newSingleThreadExecutor();
        for(int b = 0; b < batchCount; b++) {
           threadPool.execute(new BatchProcessor(collection, b, batchSize));
        }
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.MINUTES);
    }

    class BatchProcessor implements Runnable {

        private DBCollection collection;
        private int batchNum;
        private int batchSize;

        public BatchProcessor(DBCollection collection, int batchNum, int batchSize) {
            this.collection = collection;
            this.batchNum = batchNum;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            for (int i = 0; i < batchSize; i++) {
                BasicDBObject doc = new BasicDBObject();
                for (int j = 0; j < 60; j++) {
                    doc.put("key" + j, "value" + j);
                }
                doc.put("_id", "id" + batchNum + ":" + i);
                collection.save(doc);
            }
        }
    }

    @Test
    public void testTwoMongos() throws Exception {
        int batchSize = 9000;
        int batchCount = 1;

        DBCollection collection1 = db.getCollection("test-data");
        DBCollection collection2 = db2.getCollection("test-data-history");


        collection1.ensureIndex(new BasicDBObject("key0", 1));
        collection1.ensureIndex(new BasicDBObject("key1", 1));
        collection1.ensureIndex(new BasicDBObject("key2", 1));
        collection1.ensureIndex(new BasicDBObject("key3", 1));
        collection1.ensureIndex(new BasicDBObject("key4", 1));
        collection1.ensureIndex(new BasicDBObject("key5", 1));

        collection2.ensureIndex(new BasicDBObject("key0", 1));
        collection2.ensureIndex(new BasicDBObject("key1", 1));
        collection2.ensureIndex(new BasicDBObject("key2", 1));
        collection2.ensureIndex(new BasicDBObject("key3", 1));
        collection2.ensureIndex(new BasicDBObject("key4", 1));
        collection2.ensureIndex(new BasicDBObject("key5", 1));

        for (int i = 0; i < 1000; i++) {
            long before = new Date().getTime();


            ExecutorService threadPool1 = Executors.newSingleThreadExecutor();
            ExecutorService threadPool2 = Executors.newSingleThreadExecutor();

            for (int b = 0; b < batchCount; b++) {
                threadPool1.execute(new BatchProcessor(collection1, b, batchSize));
               threadPool2.execute(new BatchProcessor(collection2, b, batchSize));
            }
            threadPool1.shutdown();
            threadPool2.shutdown();
            threadPool2.awaitTermination(1, TimeUnit.MINUTES);
            threadPool1.awaitTermination(1, TimeUnit.MINUTES);


            long after = new Date().getTime();
            System.out.println("time to save (in two databases) " + (batchCount * batchSize) + " items in " + batchCount + " batches of " + batchSize + ": " + (after - before) + " msec.");
        }

    }
}
