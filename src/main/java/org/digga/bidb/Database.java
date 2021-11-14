package org.digga.bidb;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class Database {

    private Path location;
    private Map<String, Store> storeMap = new HashMap<>();

    public Database(Path location) {
        this.location = location;
    }

    Store getStore(String storeName) {
        return storeMap.get(storeName);
    }

    void putStore(Store store) {
        storeMap.put(store.getName(), store);
    }

    void closeStore(Store store) {
        storeMap.remove(store.getName());
    }

    public Path getLocation() {
        return location;
    }

    public String getDatabaseName() {
        return location.toFile().getName();
    }


}
