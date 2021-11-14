package org.digga.bidb;

import org.digga.bidb.filter.Filter;
import org.digga.bidb.index.EHCacheIndex;
import org.digga.bidb.index.IndexException;
import org.digga.bidb.index.LongIndexStore;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class EHCacheStore extends InMemoryStore {

    private static Logger log = LoggerFactory.getLogger(EHCacheStore.class);

    private ReentrantLock lock = new ReentrantLock();
    private CacheManager cacheManager;
    private Cache<Long, BiDocument> documentCache; // docId = Document

    public EHCacheStore(Database database, String storeName, boolean readonly) {
        super(database, storeName, readonly);
    }

    @Override
    public void open() throws IOException {
        lock.lock();

        try {
            log.debug("Store '"+ storeName +"' is opening...");
            CacheManagerBuilder<CacheManager> cacheBuilder = CacheManagerBuilder.newCacheManagerBuilder();
            cacheBuilder = cacheBuilder.withCache(storeName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, BiDocument.class, ResourcePoolsBuilder.heap(3000000)));
            cacheManager = cacheBuilder.build();
            cacheManager.init();
            documentCache = cacheManager.getCache(storeName, Long.class, BiDocument.class);
            longIndexStore = new LongIndexStore(database, this);

            loadFromFile();

        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addIndex(String indexName) throws IndexException {
        longIndexStore.addIndex(indexName, EHCacheIndex.class);
    }

    @Override
    public void removeIndex(String indexName) {
        longIndexStore.removeIndex(indexName);
    }

    @Override
    public void reindex(String indexName) {
        longIndexStore.removeIndex(indexName);
    }

    @Override
    public void reindexAll() {
        lock.lock();
        try {
            longIndexStore.reindexAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public UpdateResult save(BiDocument document) throws IOException {
        lock.lock();
        try {
            UpdateResult result = document.isNew() ? saveNewDocument(document) : saveDocumentWithId(document);
            longIndexStore.addIndicesFor(document);
            return result;
        } finally {
            lock.unlock();
        }
    }

    private UpdateResult saveNewDocument(BiDocument document) throws IOException {
        Long id = System.nanoTime();
        document.setId(id);
        documentCache.put(id, document);
        size++;
        return new UpdateResult(UpdateResult.UpdateStatus.INSERTED, id);
    }

    private UpdateResult saveDocumentWithId(BiDocument document) throws IOException {
        documentCache.put(document.getId(), document);
        size++;
        return new UpdateResult(UpdateResult.UpdateStatus.UPDATED, document.getId());
    }

    @Override
    public BiDocument getById(Long id) throws IOException {
        return documentCache.get(id);
    }

    @Override
    public int forEach(Consumer<BiDocument> action) {
        int docCount = 0;
        for (Cache.Entry<Long, BiDocument> entry : documentCache) {
            action.accept(entry.getValue());
            docCount++;
        }
        return docCount;
    }

    @Override
    public Cursor find(Filter filter) {
        return new SearchRequest(filter, longIndexStore, this).doSearch();
    }

    @Override
    public UpdateResult remove(Long id) {
        if (!documentCache.containsKey(id)) {
            return new UpdateResult(UpdateResult.UpdateStatus.ERROR, id, "Document with ID '" + id + "' doesn't exist");
        }

        lock.lock();
        try {
            documentCache.remove(id);
            longIndexStore.removeIndicesFor(id);
            size--;
            return new UpdateResult(UpdateResult.UpdateStatus.DELETED, id);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public UpdateResult remove(BiDocument document) {
        return remove(document.getId());
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            documentCache.clear();
            longIndexStore.clear();
            size = 0;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            saveStoreToFile();
            longIndexStore.close();
            cacheManager.close();
            database.closeStore(this);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putDocument(Long id, BiDocument document) {
        documentCache.put(id, document);
    }

}
