package org.digga.bidb;

import org.digga.bidb.filter.FieldFilter;
import org.digga.bidb.filter.Filter;
import org.digga.bidb.filter.FilterHandler;
import org.digga.bidb.filter.FilterList;
import org.digga.bidb.index.IndexStore;

import java.util.HashSet;
import java.util.Set;

public class SearchRequest extends FilterHandler {

    private IndexStore<Long> indexStore;
    private Store<Long> store;

    private Set<FieldFilter> nonIndexedFields = new HashSet<>();
    private Set<FieldFilter> indexedFields = new HashSet<>();

    public SearchRequest(Filter filter, IndexStore<Long> indexStore, Store<Long> store) {
        super(filter);
        this.indexStore = indexStore;
        this.store = store;
    }

    public Cursor doSearch() {
        separateFieldsByIndexing(indexStore);

        // for nonindexed fields go through whole collection
        if (!nonIndexedFields.isEmpty()) {
            //Timing.start("nonidx");
            store.forEach(doc -> {
                for (FieldFilter fieldFilter : nonIndexedFields) {
                    fieldFilter.match(doc);
                }
            });
            //Timing.stop("nonidx", 100);
        }

        // for indexed fields obtain data from an index
        if (!indexedFields.isEmpty()) {
            for (FieldFilter fieldFilter : indexedFields) {
                fieldFilter.apply(indexStore);
            }
        }

        traverse(f -> {
            if (f instanceof FilterList) {
                ((FilterList)f).apply();
            }
        });

        return new Cursor(store, getFilter().pointers());
    }

    private void separateFieldsByIndexing(IndexStore<Long> indexStore) {
        // find what fields are indexed and what are not
        handleLeafs(leaf -> {
            if (indexStore.hasIndex(leaf.getFieldName())) {
                indexedFields.add(leaf);
            } else {
                nonIndexedFields.add(leaf);
            }
        });
    }

}
