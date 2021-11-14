package org.digga.bidb.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class FilterList extends BaseFilter {

    private List<Filter> children;

    public FilterList(Filter... filters) {
        children = new ArrayList<>();
        children.addAll(Arrays.asList(filters));
    }

    protected List<Filter> filters() {
        return children;
    }

    public FilterList eq(String fieldName, String value) {
        children.add(new EqualsFilter(fieldName, value));
        return this;
    }

    public FilterList ne(String fieldName, String value) {
        children.add(new NotEqualsFilter(fieldName, value));
        return this;
    }

    public FilterList or(Filter... filters) {
        children.add(new OrFilter(filters));
        return this;
    }

    public void forEach(Consumer<Filter> action) {
        children.forEach(action);
    }

    public abstract void apply();

}
