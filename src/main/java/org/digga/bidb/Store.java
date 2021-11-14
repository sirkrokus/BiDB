package org.digga.bidb;

import org.digga.bidb.filter.Filter;
import org.digga.bidb.index.IndexException;

import java.io.IOException;

public interface Store<ID> extends BiDocumentCollection<ID> {

    String getName();

    Database getDatabase();

    boolean isReadonly();

    void open() throws IOException;

    void addIndex(String indexName) throws IndexException;

    void removeIndex(String indexName);

    void reindex(String indexName);

    void reindexAll();

    UpdateResult save(BiDocument document) throws IOException;

    BiDocument getById(ID id) throws IOException;

    Cursor find(Filter filter);

    UpdateResult remove(ID id);

    UpdateResult remove(BiDocument document);

    void clear();

    void close() throws IOException;

    long size();
}
