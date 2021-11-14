package org.digga.bidb;

public class LinkedHashMapStoreFactory implements StoreFactory {

    @Override
    public Store createStore(Database database, String storeName, boolean readonly) {
        return new LinkedHashMapStore(database, storeName, readonly);
    }

}
