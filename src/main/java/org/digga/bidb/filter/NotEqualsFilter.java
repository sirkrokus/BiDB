package org.digga.bidb.filter;

import org.digga.bidb.BiDocument;
import org.digga.bidb.index.Index;
import org.digga.bidb.index.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class NotEqualsFilter extends FieldFilter {

    private static Logger log = LoggerFactory.getLogger(NotEqualsFilter.class);

    public NotEqualsFilter(String fieldName, String value) {
        super(fieldName, value);
    }

    @Override
    public void match(BiDocument document) {
        String docValue = document.getString(getFieldName());
        if (log.isDebugEnabled()) {
            //log.debug("NE match [" + getFieldName() + "=" + getValue() + "] value from doc = " + document.get(getFieldName()));
        }
        boolean b = false;
        if (getValue() == null && docValue == null) {
            b = false;
        } else if (getValue() != null && !getValue().equals(docValue)) {
            b = true;
        } else if (docValue != null && !docValue.equals(getValue())) {
            b = true;
        }
        if (b) {
            addPointer(document.getId());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotEqualsFilter that = (NotEqualsFilter) o;
        return Objects.equals(getFieldName(), that.getFieldName()) && Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass().getSimpleName(), getFieldName(), getValue());
    }

    @Override
    public void apply(IndexStore<Long> indexStore) {
        Index<String, Long> index = indexStore.getIndex(getFieldName());
        addPointers(index.getPointersNE(getValue()));
    }

    @Override
    public String toString() {
        return "NE[" + getFieldName() + " != " + getValue() + "]";
    }

}
