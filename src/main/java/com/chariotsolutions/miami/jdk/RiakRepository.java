package com.chariotsolutions.miami.jdk;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.config.ClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClient;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.raw.query.indexes.BinValueQuery;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.pbc.RiakClient;

import javax.swing.event.ListSelectionEvent;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RiakRepository implements Repository {

    private static final String CURRENT_STATS_BUCKET = "current_stats";
    private static final String USER_META_STATS_KEY = "xxx-user-meta-stats-xxx";
    private static final String USER_META_STATS_KEY_DELIMITER = "|";

    //protected RawClient rawClient = null;
    protected RawClient rawClient;

    public RiakRepository() throws Exception {
        initializeRiakClient();
    }

    private void initializeRiakClient() throws Exception {
        String host = "127.0.0.1";
        int port = 8081;
        long connectionTimeout = 500;
        //RiakClient pbc_client = new RiakClient(host, port);
        //rawClient = new PBClientAdapter(pbc_client);
        //rawClient = new PBClientAdapter(host, port);

        int maxConnections = 768*2; //ClusterConfig.UNLIMITED_CONNECTIONS;

        PBClientConfig.Builder builder = new PBClientConfig.Builder();
        PBClusterConfig clusterConfig = new PBClusterConfig(maxConnections);

        clusterConfig.addClient(
                builder.withHost(host)
               .withPort(8081)
               .withConnectionTimeoutMillis(connectionTimeout)
               .build());


        clusterConfig.addClient(
                builder.withHost(host)
               .withPort(8082)
               .withConnectionTimeoutMillis(connectionTimeout)
               .build());

        clusterConfig.addClient(
                builder.withHost(host)
               .withPort(8083)
               .withConnectionTimeoutMillis(connectionTimeout)
               .build());

        clusterConfig.addClient(
                builder.withHost(host)
               .withPort(8084)
               .withConnectionTimeoutMillis(connectionTimeout)
               .build());

        clusterConfig.addClient(
                builder.withHost(host)
               .withPort(8085)
               .withConnectionTimeoutMillis(connectionTimeout)
               .build());

        clusterConfig.addClient(
                builder.withHost(host)
               .withPort(8086)
               .withConnectionTimeoutMillis(connectionTimeout)
               .build());

        rawClient = new PBClusterClient(clusterConfig);
        rawClient.generateAndSetClientId();


    }

    /**
     * Stores/updates the current value of a statistics packet.
     *
     * @param item The item to be stored.
     * @return the unique storage key for the item
     */
    public String storeCurrentItem(StatStorageItem item) throws Exception {
        String key = generateCurrentStatisticsKey(item);
        String value = "" + item.getTimestamp();
        Map<String, String> userMeta = createUserMeta(item);

        IRiakObject riakObject = RiakObjectBuilder.newBuilder(CURRENT_STATS_BUCKET, key)
                .withValue(value.getBytes())
                .withContentType("text/plain")
                .addIndex("type", item.getType())
                .addIndex("cloudid", item.getCloudId())
                .addIndex("mpid", item.getMpId())
                .addIndex("appid", item.getAppId())
                .withUsermeta(userMeta)
                .build();

        addIndicies(riakObject, item);
        
        //rawClient.store(riakObject, new StoreMeta(1, 1, 0, false, false, false, false));
        rawClient.store(riakObject);
        return key;
    }

    /**
     * adds all of the permutations of two or more indicies that will be required for
     * retrieval for roll-ups.
     * (This is because only one index can be used in a query).
     */
    private void addIndicies(IRiakObject riakObject, StatStorageItem item) {

        // *   - by type and mpid
        riakObject.addIndex("type-mpid", item.getType() + "-" + item.getMpId());

        // *   - by type, mpid, cloud
        riakObject.addIndex("type-mpid-cloudid", item.getType() + "-" + item.getMpId() + "-" + item.getCloudId());

        // *   - by type, mpid, instance
        riakObject.addIndex("type-mpid-appid", item.getType() + "-" + item.getMpId() + "-" + item.getAppId());

        // *   - by type, mpid, cloud, instance
        riakObject.addIndex("type-mpid-cloudid-appid", item.getType() + "-" + item.getMpId() + "-" + item.getCloudId() + "-" + item.getAppId());

        // *   - by type, cloud
        riakObject.addIndex("type-cloudid", item.getType() + "-" + item.getCloudId());

        // *   - by type, appId
        riakObject.addIndex("type-appid", item.getType() + "-" + item.getAppId());

        // *   - by type, cloud, instance
        riakObject.addIndex("type-cloudid-appid", item.getType() +  "-" + item.getCloudId() + "-" + item.getAppId());
    }


    /**
     * Gets the current value of a statistics packets by key.
     *
     * @param key The item's key
     * @return The current item to which the key refers.
     */
    public StatStorageItem getCurrentItemByKey(String key) throws Exception {
        RiakResponse fetched = rawClient.fetch(CURRENT_STATS_BUCKET, key);
        IRiakObject result = fetched.getRiakObjects()[0];
        StatStorageItem item = null;

        if (result != null) {
            String type = (String) result.getBinIndex("type").toArray()[0];
            Integer cloudId = (Integer) result.getIntIndex("cloudid").toArray()[0];
            Integer mpId = (Integer) result.getIntIndex("mpid").toArray()[0];
            Integer appId = (Integer) result.getIntIndex("appid").toArray()[0];
            Long value = new Long(result.getValueAsString());
            item = new StatStorageItem(type, cloudId, mpId, appId, value);
            item.setStats(getStats(result));
        }

        return item;
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
        Collection<StatStorageItem> items = new ArrayList<StatStorageItem>();
        Collection<String> keys = getCurrentItemKeys(type, cloudId, mpId, appId);
        for (String key : keys) {
            StatStorageItem item = getCurrentItemByKey(key);
            items.add(item);
        }

        return items;
    }

    /**
     * Gets all of the keys for the current statistics packets matching the parameters.
     *
     * @param type    The service type (e.g. "MEI") - Mandatory - may not be null
     * @param cloudId - The Cloud identifier - may be null
     * @param mpId    - The Market Participant identifier - may be null
     * @param appId   - The application instance id - may be null
     *                <p/>
     *                A null value for cloudId, mpId, or appId means "don't care", so for example, specifying just 'type'
     *                will get all keys the type, regardless of cloud, firm, or app instance,
     *                while specifying 'type' and 'mpId' will get keys for the specified type for the specified firm.
     */
    Collection<String> getCurrentItemKeys(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception {
        StringBuilder indexName = new StringBuilder("type");
        StringBuilder indexValue = new StringBuilder(type);

        if (mpId != null) {
            indexName.append("-").append("mpid");
            indexValue.append("-").append(mpId);
        }

        if (cloudId != null) {
            indexName.append("-").append("cloudid");
            indexValue.append("-").append(cloudId);
        }

        if (appId != null) {
            indexName.append("-").append("appid");
            indexValue.append("-").append(appId);
        }

        IndexQuery query = new BinValueQuery(BinIndex.named(indexName.toString()), CURRENT_STATS_BUCKET, indexValue.toString());
        List<String> keys = rawClient.fetchIndex(query);

        return keys;
    }

    /*
    * Note that bucket keys are cached, and the key list is not immediately updated by the delete operation,
    * so invoking this method twice in quick succession may report deletion of some number
    * of items the second time, even though they have all been deleted already.
    */
    public int deleteCurrentStats() throws Exception {
        int itemsDeleted = emptyBucket(CURRENT_STATS_BUCKET);
        return itemsDeleted;
    }

    private String generateCurrentStatisticsKey(final StatStorageItem item) {
        return new StringBuilder("T")
                .append(item.getType())
                .append("C")
                .append(item.getCloudId())
                .append("M")
                .append(item.getMpId())
                .append("A")
                .append(item.getAppId())
                .toString();
    }

    private Map<String, String> createUserMeta(final StatStorageItem item) {
        // later, when we want to retrieve the stats from the usermeta, we'll
        // need to know the keys (no way to iterate them), so we create an extra
        // entry to hold them.

        Map<String, String> userMeta = new HashMap<String, String>();
        StringBuilder keyList = new StringBuilder();
        for (String key : item.getStats().keySet()) {
            Integer intValue = item.getStats().get(key);
            userMeta.put(key, intValue.toString());

            if (keyList.length() > 0) {
                keyList.append(USER_META_STATS_KEY_DELIMITER);
            }
            keyList.append(key);
        }
        userMeta.put(USER_META_STATS_KEY, keyList.toString());
        return userMeta;
    }

    private Map<String, Integer> getStats(IRiakObject item) {
        String keyList = item.getUsermeta(USER_META_STATS_KEY);
        StringTokenizer tokenizer = new StringTokenizer(keyList, USER_META_STATS_KEY_DELIMITER);
        Map<String, Integer> stats = new HashMap<String, Integer>();
        while (tokenizer.hasMoreTokens()) {
            String key = tokenizer.nextToken();
            String value = item.getUsermeta(key);
            stats.put(key, new Integer(value));
        }
        return stats;
    }


    /**
     * Note that listKeys() is cached, and not immediately updated by the delete operation,
     * so invoking this method twice in quick succession may report deletion of some number
     * of keys the second time, even though they have all been deleted already.
     *
     * @param bucketName
     * @return
     * @throws Exception
     */
    private int emptyBucket(String bucketName) throws Exception {
        int itemsDeleted = 0;
        Iterator<String> keys = rawClient.listKeys(bucketName).iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            rawClient.delete(bucketName, key);
            itemsDeleted += 1;
        }

        return itemsDeleted;
    }


    public Collection<StatStorageItem> getCurrentItemsMultiThreaded(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception {
        Collection<StatStorageItem> items = new ArrayList<StatStorageItem>();
        String[] keys = getCurrentItemKeys(type, cloudId, mpId, appId).toArray(new String[0]);

        int CLUSTER_SIZE = 6;
        ExecutorService pool = Executors.newFixedThreadPool(CLUSTER_SIZE);
        Set<Future<Collection<StatStorageItem>>> futures = new HashSet<Future<Collection<StatStorageItem>>>();
        
        for(String key: keys) {
            Callable<Collection<StatStorageItem>> callable = new StatsRetriever(Arrays.asList(key));
            Future<Collection<StatStorageItem>> future = pool.submit(callable);
            futures.add(future);
        }


        for(Future<Collection<StatStorageItem>> future: futures) {
            items.addAll(future.get());
        }

        return items;
    }

    //// ========== multi-threading stuff ======================
    public class StatsRetriever implements Callable<Collection<StatStorageItem>> {
        private Collection<String> keys;
        private Collection<StatStorageItem> items = new ArrayList<StatStorageItem>();

        public StatsRetriever(Collection<String> itemKeys) {
            this.keys = itemKeys;
        }

        public Collection<StatStorageItem> call() {
            try {
                for (String key : keys) {
                    StatStorageItem item = getCurrentItemByKey(key);
                    items.add(item);
                }
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
            return items;
        }

        public StatStorageItem getCurrentItemByKey(String key) throws Exception {
            RiakResponse fetched = rawClient.fetch(CURRENT_STATS_BUCKET, key);
            IRiakObject result = fetched.getRiakObjects()[0];
            StatStorageItem item = null;

            if (result != null) {
                String type = (String) result.getBinIndex("type").toArray()[0];
                Integer cloudId = (Integer) result.getIntIndex("cloudid").toArray()[0];
                Integer mpId = (Integer) result.getIntIndex("mpid").toArray()[0];
                Integer appId = (Integer) result.getIntIndex("appid").toArray()[0];
                Long value = new Long(result.getValueAsString());
                item = new StatStorageItem(type, cloudId, mpId, appId, value);
                item.setStats(getStats(result));
            }

            return item;
        }

    }
}
