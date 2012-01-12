package com.chariotsolutions.miami;


import com.basho.riak.client.IRiakObject;
import com.chariotsolutions.miami.jdk.DefunctRiakRepository;
import com.chariotsolutions.miami.jdk.StatStorageItem;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DefunctRiakRepositoryTest {

    /*
    @Test
    public void testBasicStoreAndRetrieve() throws Exception {

        StatStorageItem item = new StatStorageItem("MEI", 1, 2, 3, new Date().getTime())
                .addStat("foo", 100)
                .addStat("bar", 365)
                .addProp("foo", "100")
                .addProp("bar", "365")
                .addProp("abc", "xyz");

        DefunctRiakRepository repo = new DefunctRiakRepository();
        repo.storeItem(item);

        IRiakObject riakObj = null;

        for(int x = 0; x < 100; x++)
        {
            Date before = new Date();
            int fooStat = 0;
            int barStat = 0;
            for(int i = 0; i < 768; i++) {
                 riakObj = repo.getItemByKey(item.getType(), item.getCloudId(), item.getMpId(), item.getAppId());
                 fooStat += Integer.parseInt(riakObj.getUsermeta("foo"), 10);
                 barStat += Integer.parseInt(riakObj.getUsermeta("bar"), 10);
            }
            Date after = new Date();
            System.out.println("" + (after.getTime() - before.getTime()) + " msec, sum of foo = " + fooStat + ", sum of bar = " + barStat);
        }
        assertEquals("Unexpected timestamp value", "" + item.getTimestamp(), riakObj.getValueAsString());

        assertEquals("Unexpected type", "MEI", riakObj.getBinIndex("type").toArray()[0]);
        assertEquals("Unexpected cloud", 1, riakObj.getIntIndex("cloud").toArray()[0]);
        assertEquals("Unexpected firm", 2, riakObj.getIntIndex("firm").toArray()[0]);
        assertEquals("Unexpected app", 3, riakObj.getIntIndex("app").toArray()[0]);

        assertEquals("Unexpected foo meta", "xyz", riakObj.getUsermeta("abc"));
    }

    @Test
    public void testJsonStorage() throws Exception {
        //String json = "{\"foo\":\"bar\",\"baz\":3}";
        HashMap<String, Integer> props = new HashMap<String, Integer>();
        props.put("cloud", 2);
        props.put("baz", 100);
        String json = propsToJson(props);

        DefunctRiakRepository repo = new DefunctRiakRepository();
        repo.storeJson("j4", json);

        String result = repo.getJson("j4");
        assertEquals("unexpected json", json, result);
        System.out.println("Got: " + json);
    }


    @Test
    public void storeLotsOfStats() throws Exception {
        DefunctRiakRepository repo = new DefunctRiakRepository();
        HashMap<String, Integer> props = new HashMap<String, Integer>();
        System.out.println("storing cloud 1 data");
        props.put("cloud", 1);
        for(int i = 1; i <= 10000; i++) {
            props.put("baz", i);
            String json = propsToJson(props);
            repo.storeJson("j" + i, json);
        }

        System.out.println("storing cloud2 data");
        props.put("cloud", 2);
        for(int i = 1; i <= 10000; i++) {
            props.put("baz", i*2);
            String json = propsToJson(props);
            repo.storeJson("k" + i, json);
        }
    }

    private String propsToJson(HashMap<String, Integer> props) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for(String key : props.keySet()) {
            if(first) {
              first = false;
            } else {
                json.append(", ");
            }
            json.append("\"").append(key).append("\":").append(props.get(key));
        }
        json.append("}");

        return json.toString();
    }

    @Test
    public void testGetLotsOfDataByKey() throws Exception {
        DefunctRiakRepository repo = new DefunctRiakRepository();

        for(int x = 0; x < 100; x++) {
            Date before = new Date();
            for(int i = 1; i <= 768; i++) {
               String json = repo.getJson("j"+i);
            }
            Date after = new Date();
            System.out.println( (after.getTime() - before.getTime()) + " ms.");
        }
    }

    @Test
    public void testMapReduce() throws Exception {
        List<String> mapFns = new ArrayList<String>();
        mapFns.add(new ResourceFile("/map01.js").getContent());

        List<String> reduceFns = new ArrayList<String>();
        reduceFns.add(new ResourceFile("/reduce01.js").getContent());

        DefunctRiakRepository repo = new DefunctRiakRepository();

        Date before = new Date();
        String result = repo.query(mapFns, reduceFns);
        Date after = new Date();

        //System.out.println(result);
        System.out.println(after.getTime() - before.getTime() + " ms.");
    }
    */
}
