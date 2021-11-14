package org.digga.bidb.filter;

import java.util.Collection;
import java.util.function.Consumer;

public class FilterHandler {

    private Filter filter;

    public FilterHandler(Filter filter) {
        this.filter = filter;
    }

    public Filter getFilter() {
        return filter;
    }

    public void traverse(Consumer<Filter> action) {
        traverse(filter, action);
    }

    private void traverse(Filter filter, Consumer<Filter> action) {
        if (filter == null) {
            return;
        }
        if (filter instanceof FilterList) {
            ((FilterList)filter).forEach(child -> traverse(child, action));
        }
        action.accept(filter);
    }

    // собирает только листовые узлы дерева фильтров без повторов
    // т.е. если где-то в фильтрах есть несколько одинаковых с тем же самым именем поля и
    // значением, такие фильтры попадают в результат без повторов
    public void collectLeafsTo(Collection<FieldFilter> leafCollection) {
        collectLeafsTo(filter, leafCollection);
    }

    private void collectLeafsTo(Filter filter, Collection<FieldFilter> leafCollection) {
        if (filter == null) {
            return;
        }
        if (filter instanceof FilterList) {
            ((FilterList)filter).forEach(child -> collectLeafsTo(child, leafCollection));
        } else if (filter instanceof FieldFilter) {
            leafCollection.add((FieldFilter) filter);
        }
    }

    public void handleLeafs(Consumer<FieldFilter> action) {
        handleLeafs(filter, action);
    }

    private void handleLeafs(Filter filter, Consumer<FieldFilter> action) {
        if (filter == null) {
            return;
        }
        if (filter instanceof FilterList) {
            ((FilterList)filter).forEach(child -> handleLeafs(child, action));
        } else if (filter instanceof FieldFilter) {
            action.accept((FieldFilter) filter);
        }
    }

}
