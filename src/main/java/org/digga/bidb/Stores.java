package org.digga.bidb;

import java.io.IOException;

public class Stores {

    private static StoreFactory storeFactory = new EHCacheStoreFactory();

    public static void setStoreFactory(StoreFactory factory) {
        storeFactory = factory;
    }

    public static Store openStore(Database database, String storeName) throws IOException {
        Store store = database.getStore(storeName);
        if (store == null) {
            store = storeFactory.createStore(database, storeName, false);
            database.putStore(store);
        }
        store.open();
        return store;
    }

    public static Store openReadOnlyStore(Database database, String storeName) throws IOException {
        Store store = database.getStore(storeName);
        if (store == null) {
            store = storeFactory.createStore(database, storeName, true);
            database.putStore(store);
        }
        store.open();
        return store;
    }

}
