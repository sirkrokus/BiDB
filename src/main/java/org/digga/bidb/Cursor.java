package org.digga.bidb;

import org.bson.Document;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public class Cursor {

    private Store<Long> store;
    private Set<Long> pointers;
    private BiDocumentIterator documentIterator;
    private BiDocumentIterable documentIterable;

    public Cursor(Store<Long> store, Set<Long> pointers) {
        this.store = store;
        this.pointers = pointers;
        this.documentIterator = new BiDocumentIterator();
        this.documentIterable = new BiDocumentIterable();
    }

    public int size() {
        return pointers.size();
    }

    public boolean isEmpty() {
        return pointers == null || pointers.size() == 0;
    }

    public Iterable<Document> bsonDocumentIterable() {
        return documentIterable;
    }

    public void forEach(Consumer<BiDocument> action) throws IOException {
        for (Long pointer : pointers) {
            BiDocument doc = store.getById(pointer);
            action.accept(doc);
        }
    }

    public BiDocument getFirst() throws IOException {
        for (Long pointer : pointers) {
            return store.getById(pointer);
        }
        return null;
    }

    private class BiDocumentIterable implements Iterable<Document> {

        public BiDocumentIterable() {
        }

        @Override
        public Iterator<Document> iterator() {
            return documentIterator;
        }

        @Override
        public void forEach(Consumer<? super Document> action) {
            for (Long pointer : pointers) {
                BiDocument doc = null;
                try {
                    doc = store.getById(pointer);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                action.accept(doc);
            }
        }

        @Override
        public Spliterator<Document> spliterator() {
            throw new UnsupportedOperationException("This method is not supported in " + getClass().getSimpleName());
        }

    }

    private class BiDocumentIterator implements Iterator<Document> {

        private Iterator<Long> documentIdIterator;

        public BiDocumentIterator() {
            documentIdIterator = pointers.iterator();
        }

        @Override
        public boolean hasNext() {
            return documentIdIterator.hasNext();
        }

        @Override
        public BiDocument next() {
            Long id = documentIdIterator.next();
            try {
                return store.getById(id);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            documentIdIterator.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super Document> action) {
            throw new UnsupportedOperationException("This method is not supported in " + getClass().getSimpleName());
        }

    }


}
