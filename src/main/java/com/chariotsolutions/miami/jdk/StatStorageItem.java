package com.chariotsolutions.miami.jdk;

import java.util.HashMap;
import java.util.Map;

public class StatStorageItem {
    String statType;
    String type;
    Integer cloudId;
    Integer mpId;
    Integer appId;
    long timestamp;
    Map<String, Integer> stats;
    Map<String, String> props;

    public StatStorageItem() {
        this.stats = new HashMap<String, Integer>();
        this.props = new HashMap<String, String>();
    }

    /**
     * @param statType - type of statistic, e.g. "latency", "capacity"...
     * @param type - appType, e.g. "MEI"
     * @param cloudId
     * @param mpId
     * @param appId
     * @param timestamp
     */
    public StatStorageItem(String statType, String type, Integer cloudId, Integer mpId, Integer appId, long timestamp) {
        this();
        setStatType(statType);
        setType(type);
        setCloudId(cloudId);
        setMpId(mpId);
        setAppId(appId);
        setTimestamp(timestamp);
    }

    public StatStorageItem addStat(String key, Integer value) {
        stats.put(key, value);
        return this;
    }

    public StatStorageItem addProp(String key, String value) {
        props.put(key, value);
        return this;
    }

    public String getStatType() {
        return statType;
    }

    public void setStatType(String statType) {
        this.statType = statType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getCloudId() {
        return cloudId;
    }

    public void setCloudId(Integer cloudId) {
        this.cloudId = cloudId;
    }

    public Integer getMpId() {
        return mpId;
    }

    public void setMpId(Integer mpId) {
        this.mpId = mpId;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Integer> getStats() {
        return stats;
    }

    public void setStats(Map<String, Integer> stats) {
        this.stats = stats;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(HashMap<String, String> props) {
        this.props = props;
    }
}
