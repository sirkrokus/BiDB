package org.digga.bidb.index;

import org.digga.bidb.BiDocument;
import org.digga.bidb.Database;
import org.digga.bidb.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class LongIndexStore implements IndexStore<Long> {
    private static Logger log = LoggerFactory.getLogger(LongIndexStore.class);

    private ReentrantLock lock = new ReentrantLock();
    private Database database;
    private Store<Long> store;  // docId = Document
    private Map<String, Index<String, Long>> indicesMap = new HashMap<>(); // index_name = index

    public LongIndexStore(Database database, Store<Long> store) {
        this.database = database;
        this.store = store;
    }

    @Override
    public <I extends Index> void addIndex(String indexName, Class<I> clazz) throws IndexException {
        if (indicesMap.containsKey(indexName)) {
            return;
        }
        try {
            if (log.isDebugEnabled()) {
                log.debug("Add new index '{}' as '{}'", indexName, clazz);
            }
            Index<String, Long> idx = clazz.newInstance();
            idx.init(database, store.getName(), indexName);
            indicesMap.put(indexName, idx);
            idx.open();
            reindex(indexName);
        } catch (Exception e) {
            throw new IndexException(e);
        }
    }

    @Override
    public void removeIndex(String indexName) {
        if (!indicesMap.containsKey(indexName)) {
            return;
        }
        Index<String, Long> idx = indicesMap.remove(indexName);
        idx.clear();
    }

    @Override
    public boolean hasIndex(String indexName) {
        return indicesMap.containsKey(indexName);
    }

    @Override
    public Index<String, Long> getIndex(String indexName) {
        return indicesMap.get(indexName);
    }

    @Override
    public void reindex(String indexName) {
        lock.lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Reindexing of '{}' field...", indexName);
            }
            Index<String, Long> index = indicesMap.get(indexName);
            index.clear();
            int cnt = 0;
            store.forEach(document -> {
                indexDocument(index, document);
            });
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("Index '{}' is reindexed", indexName);
            }
            lock.unlock();
        }
    }

    @Override
    public void reindexAll() {
        lock.lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Reindexing all indices...");
            }

            for (String indexName : indicesMap.keySet()) {
                Index<String, Long> index = indicesMap.get(indexName);
                index.clear();
            }

            store.forEach(document -> {
                for (String indexName : indicesMap.keySet()) {
                    Index<String, Long> index = indicesMap.get(indexName);
                    indexDocument(index, document);
                }
            });

        } finally {
            if (log.isDebugEnabled()) {
                log.debug("All indices are reindexed");
            }
            lock.unlock();
        }
    }

    @Override
    public Set<Long> getPointersForValue(String fieldName, String value) {
        return getIndex(fieldName).getPointers(value, false);
    }

    private void indexDocument(Index<String, Long> index, BiDocument doc) {
/*        indexDocumentMethodCallCount++;
        if (indexDocumentMethodCallCount % 10000 == 0) {

        }*/
        String value = doc.getString(index.name());
        value = value == null ? "_null" : value;
        index.addPointer(value, doc.getId());
    }

    public void addIndicesFor(BiDocument document) {
        for (String indexName : indicesMap.keySet()) {
            Index<String, Long> index = indicesMap.get(indexName);
            indexDocument(index, document);
        }
    }

    public void removeIndicesFor(Long id) {
        for (String indexName : indicesMap.keySet()) {
            Index<String, Long> index = indicesMap.get(indexName);
            index.removePointer(id);
        }
    }

    @Override
    public void clear() {
        for (String indexName : indicesMap.keySet()) {
            indicesMap.get(indexName).clear();
        }
    }

    @Override
    public void close() throws IOException {
        for (String indexName : indicesMap.keySet()) {
            indicesMap.get(indexName).close();
        }
    }

}
