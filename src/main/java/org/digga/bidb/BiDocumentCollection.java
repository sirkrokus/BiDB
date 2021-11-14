package org.digga.bidb;

import java.util.function.Consumer;

public interface BiDocumentCollection<ID> {

    void putDocument(ID id, BiDocument doc);

    int forEach(Consumer<BiDocument> action);

}
