package org.digga.bidb.index;

import java.util.HashSet;
import java.util.Set;

public class IndexEntry<P> { // P - position pointer in a file - Long, DocId - String or Long

    private String value; // индексируемое значение

    // указатели на позицию в базе, где находится это значение. Смещение в файле или documentId в кеше
    private HashSet<P> pointers = new HashSet<>();

    public IndexEntry() {
    }

    public IndexEntry(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public void value(String value) {
        this.value = value;
    }

    public Set<P> pointers() {
        return pointers;
    }

    public Set<P> clonePointers() {
    //    return new HashSet(pointers);
        return (Set<P>)pointers.clone();
    }

    public P firstPointer() {
        if (pointers.isEmpty()) {
            return null;
        }
        return pointers.iterator().next();
    }

}
