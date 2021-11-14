package org.digga.bidb.filter;

import org.digga.bidb.BiDocument;
import org.digga.bidb.index.Index;
import org.digga.bidb.index.IndexStore;
import org.digga.bidb.utils.Timing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class EqualsFilter extends FieldFilter {

    private static Logger log = LoggerFactory.getLogger(EqualsFilter.class);

    public EqualsFilter(String fieldName, String value) {
        super(fieldName, value);
    }

    @Override
    public void match(BiDocument document) {
        boolean b = Objects.equals(getValue(), document.get(getFieldName()));
        if (log.isDebugEnabled()) {
            //log.debug("EQ match [" + getFieldName() + "=" + getValue() + "] value from doc = " + document.get(getFieldName()));
        }
        if (b) {
            addPointer(document.getId());
        }
    }

    @Override
    public void apply(IndexStore<Long> indexStore) {
        Index<String, Long> index = indexStore.getIndex(getFieldName());
        Timing.start("addPointers");
        addPointers(index.getPointers(getValue(), true));
        Timing.stop("addPointers", 2000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EqualsFilter that = (EqualsFilter) o;
        return Objects.equals(getFieldName(), that.getFieldName()) && Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass().getSimpleName(), getFieldName(), getValue());
    }

    @Override
    public String toString() {
        return "EQ[" + getFieldName() + " = " + getValue() + "]";
    }

}
