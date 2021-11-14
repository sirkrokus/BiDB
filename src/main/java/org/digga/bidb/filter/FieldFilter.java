package org.digga.bidb.filter;

import org.digga.bidb.BiDocument;
import org.digga.bidb.index.IndexStore;

public abstract class FieldFilter extends BaseFilter {

    private String fieldName;
    private String value;

    public FieldFilter(String fieldName, String value) {
        this.fieldName = fieldName;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getValue() {
        return value;
    }

    public abstract void match(BiDocument document);

    public abstract void apply(IndexStore<Long> indexStore);

}
