package org.digga.bidb.index;

import org.digga.bidb.Database;

import java.io.IOException;
import java.util.Set;

public interface Index<V, P> { // V - indexed value (string), P - pointers (docId) to a value (long, string)

    void init(Database database, String storeName, String indexName);

    String name();

    long getNumberOfEntries();

    void open() throws IOException;

    void addPointer(V value, P pointer);

    Set<P> getPointers(V value, boolean clonedValues);

    Set<P> getPointersNE(V value);

    P getPointer(V value);

    void removePointer(P pointer);

    void remove(V value);

    void clear();

    void close() throws IOException;

}
