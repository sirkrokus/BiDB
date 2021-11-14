package org.digga.bidb.index;

import java.io.IOException;
import java.util.Set;

public interface IndexStore<P> {

    <I extends Index> void addIndex(String indexName, Class<I> clazz) throws IndexException;

    void removeIndex(String indexName);

    boolean hasIndex(String indexName);

    Index<String, P> getIndex(String indexName);

    void reindex(String indexName);

    void reindexAll() throws IOException;

    Set<P> getPointersForValue(String fieldName, String value);

    void clear();

    void close() throws IOException;
}
