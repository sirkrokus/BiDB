package org.digga.bidb.filter;

public class AndFilter extends FilterList {

    public AndFilter(Filter... filters) {
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
            first.pointers().retainAll(filter.pointers());
        }
        addPointers(first.pointers());
    }

    @Override
    public String toString() {
        return "AND["+filters().size()+"]";
    }

}
