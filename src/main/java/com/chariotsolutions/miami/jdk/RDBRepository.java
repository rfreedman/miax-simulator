package com.chariotsolutions.miami.jdk;

import org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp.datasources.SharedPoolDataSource;
import org.java.util.concurrent.NotifyingBlockingThreadPoolExecutor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RDBRepository implements Repository {

    private static DataSource dataSource;

    private static ThreadPoolExecutor threadPool;

    private static final Set<String> KNOWN_STATS = new HashSet<String>();

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
            tds.setMaxActive(100);
            tds.setMaxWait(100);

            dataSource = tds;

            testConnection();
            
            initThreadPool();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void testConnection() throws Exception {
        Connection connection = null;

        try {
            connection = getConnection();
            Statement statement = connection.createStatement();
            statement.execute("select 1 from dual");
            System.out.println("db connection validated");
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
            storeCurrentStatsDynamic(item);
        } else {
            System.out.println("unknown stat: " + item.getStatName());
        }
        return "";
    }

    private void storeCurrentStatsDynamic(StatStorageItem item) throws Exception {
        Connection connection = null;
        try {
            connection = getConnection();

            PreparedStatement updateStatement = connection.prepareStatement(createUpdateSql(item));
            bindUpdateStatement(item, updateStatement);
            int rowsUpdated = updateStatement.executeUpdate();

            if (rowsUpdated == 0) {
                PreparedStatement insertStatement = connection.prepareStatement(createInsertSql(item));
                bindInsertStatement(item, insertStatement);
                insertStatement.executeUpdate();
            }

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


    private static void initThreadPool() {
        int poolSize = 10;
        int queueSize = 10000;
        int threadKeepAliveTime = 10;
        TimeUnit threadKeepAliveTimeUnit = TimeUnit.MILLISECONDS;
        int maxBlockingTime = 100;
        TimeUnit maxBlockingTimeUnit = TimeUnit.MILLISECONDS;

        Callable<Boolean> blockingTimeoutCallback = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                System.out.println("waiting for task insertion...");
                return true;
            }
        };


        threadPool = new NotifyingBlockingThreadPoolExecutor(
                poolSize,
                queueSize,
                threadKeepAliveTime, threadKeepAliveTimeUnit,
                maxBlockingTime, maxBlockingTimeUnit,
                blockingTimeoutCallback
        );
    }

    class Updater implements Runnable {

        private String type;
        private int cloud;
        private int instance;
        private int mpid;
        private int cap_100;

        public Updater(String type, int cloud, int instance, int mpid, int cap_100) {
            this.type = type;
            this.cloud = cloud;
            this.instance = instance;
            this.mpid = mpid;
            this.cap_100 = cap_100;
        }

        public void run() {
            Connection connection = null;
            try {
                connection = getConnection();

                PreparedStatement updateStatement = connection.prepareStatement("update mei_capacity set cap_100 = ? where type = ? and cloud = ? and instance = ? and mpid = ?");

                updateStatement.setInt(1, cap_100);
                updateStatement.setString(2, type);
                updateStatement.setInt(3, cloud);// cloud
                updateStatement.setInt(4, instance); // instance
                updateStatement.setInt(5, mpid); // mpid
                int rowsUpdated = updateStatement.executeUpdate();

                if (rowsUpdated == 0) {
                    PreparedStatement insertStatement = connection.prepareStatement("insert into mei_capacity(type, cloud, instance, mpid, cap_100) values (?, ?, ?, ?, ?)");
                    insertStatement.setString(1, type);
                    insertStatement.setInt(2, cloud);// cloud
                    insertStatement.setInt(3, instance); // instance
                    insertStatement.setInt(4, mpid); // mpid
                    insertStatement.setInt(5, cap_100); // cap_100
                    insertStatement.executeUpdate();
                }

            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
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
    }
}
