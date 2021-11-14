package org.digga.bidb.index;

import org.digga.bidb.Database;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class EHCacheIndex implements Index<String, Long> {

    private static Logger log = LoggerFactory.getLogger(EHCacheIndex.class);

    public static final String IDX_EXT = ".idx";
    private static final String TMP_IDX_EXT = ".tmp";

    private ReentrantLock lock = new ReentrantLock();
    private Database database;
    private String storeName;
    private String indexName;
    private long maxEntries = 3000000L; // possible (max) number of entries

    private CacheManager cacheManager;
    private Cache<String, IndexEntry> cache; // entries
    private long numberOfEntries; // actually added number of entries

    public EHCacheIndex() {
    }

    @Override
    public void init(Database database, String storeName, String indexName) {
        this.database = database;
        this.storeName = storeName;
        this.indexName = indexName;
    }

    @Override
    public String name() {
        return indexName;
    }

    @Override
    public long getNumberOfEntries() {
        return numberOfEntries;
    }

    @Override
    public void open() throws IOException {
        lock.lock();

        try {
            if (log.isDebugEnabled()) {
                log.debug("Open '{}' index", indexName);
            }
            CacheManagerBuilder<CacheManager> cacheBuilder = CacheManagerBuilder
                    .newCacheManagerBuilder()
                    .withCache(indexName, CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, IndexEntry.class, ResourcePoolsBuilder.heap(maxEntries)));
            cacheManager = cacheBuilder.build();
            cacheManager.init();
            cache = cacheManager.getCache(indexName, String.class, IndexEntry.class);

            // Reload indexed data from a file
            Path dataPath = Paths.get(database.getLocation().toString(), storeName + "_" + indexName + IDX_EXT);
            File dataFile = dataPath.toFile();
            log.debug("Try to reload the index from " + dataFile);
            if (!dataFile.exists()) {
                log.info("Index '" + storeName + "_" + indexName + "' doesn't exist");
                return;
            }

            //RandomAccessFile dataStream = new RandomAccessFile(dataFile, "r");
            //dataStream.seek(0);
            ObjectInputStream dataStream = new ObjectInputStream(new FileInputStream(dataFile));
            numberOfEntries = dataStream.readLong();
            for (int i = 0; i < numberOfEntries; i++) {
                String value = dataStream.readUTF();
                IndexEntry idx = new IndexEntry(value);
                long numpos = dataStream.readLong();
                for (int p = 0; p < numpos; p++) {
                    Long pos = dataStream.readLong();
                    idx.pointers().add(pos);
                }
                cache.put(value, idx);
            }
            dataStream.close();
            log.info(numberOfEntries + " indexed values(s) are loaded");

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addPointer(String value, Long pos) {
        checkCache();
        lock.lock();
        try {
            IndexEntry<Long> indexEntry;
            if (!cache.containsKey(value)) {
                indexEntry = new IndexEntry(value);
                cache.put(value, indexEntry);
                numberOfEntries++;
            } else {
                indexEntry = cache.get(value);
            }

            indexEntry.pointers().add(pos);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<Long> getPointers(String value, boolean clonedValues) {
        checkCache();
        if (!cache.containsKey(value)) {
            return Collections.EMPTY_SET;
        }
        return clonedValues ? cache.get(value).clonePointers() : cache.get(value).pointers();
    }

    @Override
    public Set<Long> getPointersNE(String value) {
        checkCache();
        HashSet<Long> pointerSet = new HashSet<>();
        cache.forEach(cacheEntry -> {
            if (cacheEntry.getKey().equals(value)) {
                return;
            }
            pointerSet.addAll(cacheEntry.getValue().pointers());
        });
        return pointerSet;
    }

    @Override
    public Long getPointer(String value) {
        checkCache();
        if (!cache.containsKey(value)) {
            return null;
        }
        return (Long)cache.get(value).firstPointer();
    }

    @Override
    public void removePointer(Long pointer) {
        for (Cache.Entry<String, IndexEntry> entry : cache) {
            IndexEntry<Long> idx = (IndexEntry<Long>)entry.getValue();
            idx.pointers().remove(pointer);
        }
    }

    @Override
    public void remove(String value) {
        checkCache();
        lock.lock();
        try {
            if (!cache.containsKey(value)) {
                return;
            }
            cache.get(value).pointers().clear();
            cache.remove(value);
            numberOfEntries--;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        checkCache();
        lock.lock();
        try {
            cache.clear();
            numberOfEntries = 0;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            // Save indexed data to a file
            Path tmpPath = Paths.get(database.getLocation().toString(), storeName + "_" + indexName + TMP_IDX_EXT);
            File tmpFile = tmpPath.toFile();
            log.debug("Try to save the index to " + tmpFile);

            int recCount = 0;
            // RandomAccessFile dataStream = new RandomAccessFile(tmpFile, "rw");
            ObjectOutputStream dataStream = new ObjectOutputStream(new FileOutputStream(tmpFile));
            dataStream.writeLong(numberOfEntries);
            for (Cache.Entry<String, IndexEntry> entry : cache) {
                IndexEntry<Long> idx = entry.getValue();
                dataStream.writeUTF(idx.value());
                dataStream.writeLong(idx.pointers().size());
                Iterator<Long> it = idx.pointers().iterator();
                while (it.hasNext()) {
                    Long pos = it.next();
                    dataStream.writeLong(pos);
                }
                recCount++;
            }
            dataStream.close();
            log.info(recCount + " index(es) are saved");

            Path dataPath = Paths.get(database.getLocation().toString(), storeName + "_" + indexName + IDX_EXT);
            File dataFile = dataPath.toFile();
            if (dataFile.exists()) {
                dataFile.delete();
            }
            boolean r = tmpFile.renameTo(dataFile);
            if (!r) {
                log.error("Impossible to rename a temporary file " + tmpFile + ". Data is not saved");
            }
            cacheManager.close();

        } finally {
            lock.unlock();
        }

    }

    private void checkCache() {
        if (cache == null) {
            throw new RuntimeException("Index '" + storeName + "." + indexName + "' is not opened yet");
        }
    }

}
