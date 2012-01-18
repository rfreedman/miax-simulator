package com.chariotsolutions.miami.jdk;

import java.util.Collection;
import java.util.Map;

import com.mongodb.*;

public class MongoRepository implements Repository {

    private Mongo mongo = new Mongo();
    private DB db = mongo.getDB("miax-stats");

    public MongoRepository() throws Exception {

    }
    /**
     * Stores/updates the current value of a statistics packet.
     *
     * @param item The item to be stored.
     * @return the unique storage key for the item
     */
    public String storeCurrentItem(StatStorageItem item) throws Exception {
        /*
        BasicDBObject doc = storageItemToDBObject(item);
        DBCollection collection = db.getCollection("current_stats");

        if(collection.getCount() == 0) {
            collection.ensureIndex(new BasicDBObject("type", 1));
            collection.ensureIndex(new BasicDBObject("cloud", 1));
            collection.ensureIndex(new BasicDBObject("instance", 1));
            collection.ensureIndex(new BasicDBObject("mpid", 1));
            collection.ensureIndex(new BasicDBObject("stat", 1));
        }
        collection.save(doc);


       /// DBObject error  = db.getLastError();
       // if(error.containsField("err") && error.get("err") != null) {
       //     throw new Exception((String)error.get("err"));
       // }


        return (String) doc.get("_id");
        */
        return insertOrUpdateCurrentItem(item);
    }

    public String insertOrUpdateCurrentItem(StatStorageItem item) throws Exception {
        BasicDBObject doc = storageItemToDBObject(item);
        DBCollection collection = db.getCollection("current_stats");

        if(collection.getCount() == 0) {
            collection.ensureIndex(new BasicDBObject("type", 1));
            collection.ensureIndex(new BasicDBObject("cloud", 1));
            collection.ensureIndex(new BasicDBObject("instance", 1));
            collection.ensureIndex(new BasicDBObject("mpid", 1));
            collection.ensureIndex(new BasicDBObject("stat", 1));
        }

        //collection.save(doc);
        BasicDBObject query = new BasicDBObject("_id", doc.get("_id"));
        DBObject existingObject = collection.findOne(query);
        if(existingObject == null) {
            System.out.println("inserting object: " + doc.get("_id"));
            collection.insert(doc);
        } else {
            System.out.println("updating object: " + doc.get("_id"));
            existingObject.put("stats", doc.get("stats"));
            existingObject.put("timestamp", doc.get("timestamp"));

            // if we want to store some running aggregates for the day, we can add them here

            collection.update(query, existingObject);
        }

        return (String) doc.get("_id");
    }

    public BasicDBObject storageItemToDBObject(StatStorageItem item) {
        BasicDBObject doc = new BasicDBObject();
        doc.put("_id", makeKey(item));;
        doc.put("type", item.type);
        doc.put("cloud", item.cloudId);
        doc.put("instance", item.appId);
        doc.put("mpid", item.mpId);
        doc.put("stat", item.statType);
        doc.put("timestamp", item.timestamp);

        BasicDBObject stats = new BasicDBObject();
        for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
            stats.append(entry.getKey(), entry.getValue());
        }
        doc.put("stats", stats);
        return doc;
    }

    // todo: add statistics type
    private String makeKey(StatStorageItem item) {
        return "t:" + item.type + "|" + "c:" + item.cloudId + "|" + "i:" + item.appId + "|" + "m:" + item.mpId + "|" + "s:" + item.statType;
    }

    /**
     * Gets the current value of a statistics packets by key.
     *
     * @param key The item's key
     * @return The current item to which the key refers.
     */
    public StatStorageItem getCurrentItemByKey(String key) throws Exception {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Gets all of the current statistics packets matching the parameters.
     *
     * @param type    The service type (e.g. "MEI")
     * @param cloudId - The Cloud identifier
     * @param mpId    - The Market Participant identifier
     * @param appId   - The application instance id
     *                <p/>
     *                A null value for cloudId, mpId, or appId means "don't care", so for example, specifying just 'type'
     *                will get all stats packages for the type, regardless of cloud, firm, or app instance,
     *                while specifying 'type' and 'mpId' will get stats for the specified type for the specified firm.
     */
    public Collection<StatStorageItem> getCurrentItems(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
