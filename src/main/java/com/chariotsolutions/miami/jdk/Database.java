package com.chariotsolutions.miami.jdk;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds all the stats data in the system
 */
public class Database {
    private Map<Integer, AppInstance> data = new HashMap<Integer, AppInstance>();

    public synchronized AppInstance getAppInstance(int applicationId) {
        AppInstance instance = data.get(applicationId);
        if(instance == null) {
            instance = new AppInstance();
            data.put(applicationId, instance);
        }
        return instance;
    }
}


