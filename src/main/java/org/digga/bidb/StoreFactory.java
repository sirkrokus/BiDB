package org.digga.bidb;

public interface StoreFactory {

    Store createStore(Database database, String storeName, boolean readonly);

}
