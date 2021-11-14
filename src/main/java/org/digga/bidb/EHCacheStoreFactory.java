package org.digga.bidb;

public class EHCacheStoreFactory implements StoreFactory {

    @Override
    public Store createStore(Database database, String storeName, boolean readonly) {
        return new EHCacheStore(database, storeName, readonly);
    }

}
