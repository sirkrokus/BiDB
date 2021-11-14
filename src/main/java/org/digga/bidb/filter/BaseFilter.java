package org.digga.bidb.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class BaseFilter implements Filter {

    private Set<Long> pointers = new HashSet<>();

    protected void addPointers(Collection<Long> pointers) {
        this.pointers.addAll(pointers);
    }

    protected void addPointer(Long pointer) {
        this.pointers.add(pointer);
    }

    public Set<Long> pointers() {
        return pointers; // массив указателей на ID документов
    }

}
