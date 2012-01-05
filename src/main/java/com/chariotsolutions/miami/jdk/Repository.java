package com.chariotsolutions.miami.jdk;


import java.util.Collection;

public interface Repository {
    /**
     * Stores/updates the current value of a statistics packet.
     * @param item The item to be stored.
     * @return the unique storage key for the item
     **/
    public String storeCurrentItem(StatStorageItem item) throws Exception;

    /**
     * Gets the current value of a statistics packets by key.
     * @param key The item's key
     * @return The current item to which the key refers.
     */
    public StatStorageItem getCurrentItemByKey(String key) throws Exception;


    /**
     * Gets all of the current statistics packets matching the parameters.
     * @param type The service type (e.g. "MEI")
     * @param cloudId - The Cloud identifier
     * @param mpId - The Market Participant identifier
     * @param appId - The application instance id
     * 
     * A null value for cloudId, mpId, or appId means "don't care", so for example, specifying just 'type'
     * will get all stats packages for the type, regardless of cloud, firm, or app instance,
     * while specifying 'type' and 'mpId' will get stats for the specified type for the specified firm.
     **/
    public Collection<StatStorageItem> getCurrentItems(String type, Integer cloudId, Integer mpId, Integer appId) throws Exception;

}
