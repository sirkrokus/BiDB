package org.digga.bidb.filter;

public class OrFilter extends FilterList {

    public OrFilter(Filter... filters) {
        super(filters);
    }

    @Override
    public void apply() {
        Filter first = null;
        for (Filter filter : filters()) {
            if (first == null) {
                first = filter;
                continue;
            }
            first.pointers().addAll(filter.pointers());
        }
        addPointers(first.pointers());
    }

    @Override
    public String toString() {
        return "OR["+filters().size()+"]";
    }

}
