
package com.chariotsolutions.miami;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

public class MongoCopyCollectionTest {

    private Mongo mongo;
    private DB db;

    @Before
    public void setUp() throws Exception {
        mongo = new Mongo();
        db = mongo.getDB("copytest");
        db.dropDatabase();
    }

    @After
    public void tearDown() {
        db.dropDatabase();
    }

    @Test
    public void testCopyCollection() throws Exception {

        DBCollection primaryCollection = db.getCollection("primary");

        System.out.println("inserting 9,000 objects");

        long before = new Date().getTime();
        for(int i = 0; i < 9000; i++) {
            BasicDBObject obj = new BasicDBObject();
            obj.put("type", "mei");
            obj.put("cloud", "1");
            obj.put("instance", "1");
            obj.put("mpId", "1");
            for(int j = 0; j < 60; j++) {
                obj.put("stat-" + j, j);
            }
            primaryCollection.save(obj);
        }
        long after = new Date().getTime();

        System.out.println("done inserting: " + (after - before ) + " msec");

        for(int i =1; i <= 100; i++) {
           before = new Date().getTime();

            db.eval("db.primary.find().forEach( function(x){db.secondary.insert(x)} );");
            after = new Date().getTime();

            System.out.println("time to copy: " + (after - before) + " msec.");

            assert(db.getCollection("secondary").getCount() == 9000 * i);
        }
     }


    @Test
    public void testInsertTwice() {
        DBCollection primaryCollection = db.getCollection("primary");
        DBCollection secondaryCollection = db.getCollection("secondary");
        System.out.println("inserting 9,000 objects");

        for (int k = 0; k < 100; k++) {
            long before = new Date().getTime();
            for (int i = 0; i < 9000; i++) {
                BasicDBObject obj = new BasicDBObject();
                obj.put("type", "mei");
                obj.put("cloud", "1");
                obj.put("instance", "1");
                obj.put("mpId", "1");
                for (int j = 0; j < 60; j++) {
                    obj.put("stat-" + j, j);
                }
                primaryCollection.save(obj);
                secondaryCollection.insert(obj);
            }
            long after = new Date().getTime();
            System.out.println("time to insert into two collections: " + (after - before) + " msec.");
        }
    }
}
