package com.chariotsolutions.miami.jdk;

import org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp.datasources.SharedPoolDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RDBRepository implements Repository {

    class BatchUpdater implements Runnable {
        private static final int MAX_BATCH_SIZE = 50;
        private final Queue<StatStorageItem> items = new ConcurrentLinkedQueue<StatStorageItem>();
        private String statName = null;

        public BatchUpdater(String statName) {
            this.statName = statName;
        }

        public void addItem(StatStorageItem item) {
            items.add(item);
        }

        public void run() {
            while(true) {
                Connection connection = null;
                long before = new Date().getTime();
                try {
                    connection = getConnection();
                    long afterConnection = new Date().getTime();

                    PreparedStatement replaceStatement = null;
                    int batchSize = 0;

                    for(int i = 0; i < MAX_BATCH_SIZE; i++) {

                        StatStorageItem item = items.poll();
                        if(item == null) {
                            break;
                        }

                        if(replaceStatement == null) {
                            replaceStatement = connection.prepareStatement(createReplaceSql(item));
                        }

                        bindReplaceStatement(item, replaceStatement);
                        replaceStatement.addBatch();
                        batchSize += 1;
                    }

                    if(batchSize > 0) {
                        replaceStatement.executeUpdate();
                        long afterReplace = new Date().getTime();
                        System.out.println("storage of " + batchSize + " " + statName + " items took " + (afterReplace - before) + " msec. including " + (afterConnection - before) + " msec. for connection");
                    }

                } catch(Exception ex) {
                    ex.printStackTrace();
                }  finally {
                    if(connection != null) {
                        try{connection.close();} catch(Exception ex){}
                    }
                }

                //try{Thread.sleep(50);}catch(InterruptedException ex){}
            }
        }
    }

    private static DataSource dataSource;

    private static final Set<String> KNOWN_STATS = new HashSet<String>();

    private Map<String, BatchUpdater> batchUpdaters = new HashMap<String, BatchUpdater>();

    static {
        try {
            KNOWN_STATS.add("STAT_BULK_QUOTE_CAPACITY_MSEC"); // todo - get this from configuration
            KNOWN_STATS.add("STAT_BULK_QUOTE_REQ_LATENCY_NSEC");
            KNOWN_STATS.add("STAT_BULK_QUOTE_RESP_LATENCY_NSEC");
            KNOWN_STATS.add("STAT_BULK_QUOTE_RTT_LATENCY_NSEC");
            KNOWN_STATS.add("MEI_CUSTOM");
            KNOWN_STATS.add("SESME_CUSTOM");


            DriverAdapterCPDS cpds = new DriverAdapterCPDS();
            cpds.setDriver("org.gjt.mm.mysql.Driver");

            /*
            cpds.setUrl("jdbc:mysql://localhost:3306/miax");
            cpds.setUser("miax");
            cpds.setPassword("miax");
            */

            cpds.setUrl("jdbc:mysql://dpr1d1bps08:3306/user");
            cpds.setUser("rfreedman");
            cpds.setPassword("welcome1");


            cpds.setPoolPreparedStatements(true);


            SharedPoolDataSource tds = new SharedPoolDataSource();
            tds.setConnectionPoolDataSource(cpds);
            tds.setMaxActive(200);  // 100
            tds.setMaxWait(100);

            dataSource = tds;

            testConnection();
            
            //initThreadPool();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void testConnection() throws Exception {
        Connection connection = null;
        System.out.println("starting connection test: " + new Date());
        try {
            connection = getConnection();
            Statement statement = connection.createStatement();
            statement.execute("select 1 from dual");
            System.out.println("db connection validated: " + new Date());
        } catch(Exception ex) {
            System.out.println("db connection validation failed: " + new Date());
        } finally {
            if(connection != null) {
                try{connection.close();}catch(Exception ex){}
            }
        }
    }

    public static Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    /**
     * Stores/updates the current value of a statistics packet.
     *
     * @param item The item to be stored.
     * @return the unique storage key for the item
     */
    @Override
    public String storeCurrentItem(StatStorageItem item) throws Exception {

        if(KNOWN_STATS.contains(item.getStatName())) {
            //storeCurrentStatsDynamic(item);
            batchItem(item);
        } else {
            System.out.println("unknown stat: " + item.getStatName());
        }
        return "";
    }

    private void batchItem(StatStorageItem item) {
        BatchUpdater updater = batchUpdaters.get(item.getStatName());
        if(updater == null) {
            updater = new BatchUpdater(item.getStatName());
            batchUpdaters.put(item.getStatName(), updater);
            new Thread(updater).start();
        }
        updater.addItem(item);
    }

    private void storeCurrentStatsDynamic(StatStorageItem item) throws Exception {
        Connection connection = null;
        try {
            long before = new Date().getTime();
            connection = getConnection();
            long afterConnection = new Date().getTime();

            /*
            PreparedStatement updateStatement = connection.prepareStatement(createUpdateSql(item));
            bindUpdateStatement(item, updateStatement);
            int rowsUpdated = updateStatement.executeUpdate();

            if (rowsUpdated == 0) {
                PreparedStatement insertStatement = connection.prepareStatement(createInsertSql(item));
                bindInsertStatement(item, insertStatement);
                insertStatement.executeUpdate();
            }
            */
            PreparedStatement replaceStatement = connection.prepareStatement(createReplaceSql(item));
            bindReplaceStatement(item, replaceStatement);
            replaceStatement.executeUpdate();
            long afterReplace = new Date().getTime();
            System.out.println("storage took " + (afterReplace - before) + " msec. including " + (afterConnection - before) + " msec. for connection");


        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(item.getStatName() + ": " + item.getStats());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) {
                    //
                }
            }
        }
    }


    private String createReplaceSql(StatStorageItem item)  {
        StringBuilder replaceSql = new StringBuilder("replace into ")
                .append(item.getStatName())
                .append("(type, cloud, instance, mpid, timestamp");

        for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
            replaceSql.append(", ").append(entry.getKey());
        }
        replaceSql.append(") values ( ?");

        int cols = item.getStats().size() + 4;
        for(int i = 0; i < cols; i++) {
            replaceSql.append(", ?");
        }

        replaceSql.append(");");

        return replaceSql.toString();
    }

    private void bindReplaceStatement(StatStorageItem item, PreparedStatement replaceStatement) throws Exception{
         int col = 0;

         replaceStatement.setString(++col, item.type);
         replaceStatement.setInt(++col, item.getCloudId());// cloud
         replaceStatement.setInt(++col, item.getAppId()); // instance
         replaceStatement.setInt(++col, item.getMpId()); // mpid
         replaceStatement.setLong(++col, item.getTimestamp());

         for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
             replaceStatement.setInt(++col, entry.getValue());
         }

     }

    // TODO: get rid of 'type' as a column - not needed

    private String createUpdateSql(StatStorageItem item) {
        StringBuilder updateSql = new StringBuilder("update ")
               .append(item.getStatName())
               .append(" set ");

       for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
          updateSql.append(" ")
                  .append(entry.getKey())
                  .append(" = ?, ");
       }
       updateSql.append(" timestamp = ? where type = ? and cloud = ? and instance = ? and mpid = ?");
       return updateSql.toString();
    }

    private void bindUpdateStatement(StatStorageItem item, PreparedStatement updateStatement) throws Exception {
        int col = 0;

        for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
            updateStatement.setInt(++col, entry.getValue());
        }

        updateStatement.setLong(++col, item.getTimestamp());
        updateStatement.setString(++col, item.type);
        updateStatement.setInt(++col, item.getCloudId());// cloud
        updateStatement.setInt(++col, item.getAppId()); // instance
        updateStatement.setInt(++col, item.getMpId()); // mpid
    }

    private String createInsertSql(StatStorageItem item) {
        StringBuilder insertSql = new StringBuilder("insert into ")
               .append(item.getStatName())
               .append("(type, cloud, instance, mpid, timestamp");

        for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
            insertSql.append(", ").append(entry.getKey());
        }
        insertSql.append(") values ( ?");

        int cols = item.getStats().size() + 4;
        for(int i = 0; i < cols; i++) {
            insertSql.append(", ?");
        }
        insertSql.append(");");

        return insertSql.toString();
    }

    private void bindInsertStatement(StatStorageItem item, PreparedStatement insertStatement) throws Exception{
        int col = 0;

        insertStatement.setString(++col, item.type);
        insertStatement.setInt(++col, item.getCloudId());// cloud
        insertStatement.setInt(++col, item.getAppId()); // instance
        insertStatement.setInt(++col, item.getMpId()); // mpid
        insertStatement.setLong(++col, item.getTimestamp());

        for(Map.Entry<String, Integer> entry: item.getStats().entrySet()) {
            insertStatement.setInt(++col, entry.getValue());
        }

    }

    /**
     * Gets the current value of a statistics packets by key.
     *
     * @param key The item's key
     * @return The current item to which the key refers.
     */
    @Override
    public StatStorageItem getCurrentItemByKey(String key) throws Exception {
        return null; 
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
    @Override
    public Collection<StatStorageItem> getCurrentItems(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception {
        return null;
    }
}
