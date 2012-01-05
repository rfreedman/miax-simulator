package com.chariotsolutions.miami.jdk;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.BucketMapReduce;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.JSSourceFunction;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.pbc.RiakClient;

import java.util.List;

public class DefunctRiakRepository {

    protected RawClient rawClient = null;

    public DefunctRiakRepository() throws Exception {
        initializeRiakClient();
    }

    private void initializeRiakClient() throws Exception {
        String host = "127.0.0.1";
        int port = 8081;
        RiakClient pbc_client = new RiakClient(host, port);
        rawClient = new PBClientAdapter(pbc_client);
    }

    public void storeItem(StatStorageItem item) throws Exception {
        String bucket = "current_stats";

        String key = "T:" + item.getType() + ":C:" + item.getCloudId() + ":F:" + item.getMpId() + ":A:" + item.getAppId();
        String value = "" + item.getTimestamp();
        IRiakObject riakObject = RiakObjectBuilder.newBuilder(bucket, key)
                .withValue(value.getBytes())
                .withContentType("text/plain")
                .addIndex("type", item.getType())
                .addIndex("cloud", item.getCloudId())
                .addIndex("firm", item.getMpId())
                .addIndex("app", item.getAppId())
                .withUsermeta(item.getProps())
                .build();



        //rawClient.store(riakObject, new StoreMeta(1, 1, 0, false, false, false, false));
        rawClient.store(riakObject);
    }

    public void storeJson(String key, String json) throws Exception {
        String bucket = "json_test";
        IRiakObject riakObject = RiakObjectBuilder.newBuilder(bucket, key)
                .withValue(json.getBytes())
                .build();
        rawClient.store(riakObject);
    }

    public String getJson(String key) throws Exception {
        String bucket = "json_test";
         RiakResponse fetched = rawClient.fetch(bucket, key);
         IRiakObject result = fetched.getRiakObjects()[0];
         return result.getValueAsString();
    }

    public IRiakObject getItemByKey(String type, Integer cloudId, Integer firmId, Integer appId) throws Exception {
        String bucket = "current_stats";
        String key = "T:" + type + ":C:" + cloudId + ":F:" + firmId + ":A:" + appId;

        RiakResponse fetched = rawClient.fetch(bucket, key);
/*
	    IRiakObject result = null;
	    if (fetched.hasValue()) {
	        if (fetched.hasSiblings()) {
	            //do what you must to resolve conflicts
	            throw new RuntimeException("Unhandled conflict!");
	        }
	        else {
	            result = fetched.getRiakObjects()[0];
	        }
	    }
*/

        IRiakObject result = fetched.getRiakObjects()[0];
        return result;
    }


    public String query(List<String> mapFunctions, List<String> reduceFunctions) throws Exception {
        String bucket = "json_test";
        BucketMapReduce mapReduce = new BucketMapReduce(rawClient, bucket);

        for(String functionSource : mapFunctions) {
            JSSourceFunction jsSourceFunction = new JSSourceFunction(functionSource);
            mapReduce.addMapPhase(jsSourceFunction, true);
        }


        for(String functionSource : reduceFunctions) {
            JSSourceFunction jsSourceFunction = new JSSourceFunction(functionSource);
            //mapReduce.addReducePhase(jsSourceFunction);
        }
        

        MapReduceResult result =  mapReduce.execute();
        return ""; //result.getResultRaw();
    }

}
