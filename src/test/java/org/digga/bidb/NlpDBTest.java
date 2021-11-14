package org.digga.bidb;

import org.digga.bidb.filter.Filter;
import org.digga.bidb.filter.Filters;
import org.digga.bidb.utils.TextUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class NlpDBTest {

    private String[] words = new String[] {
            "miza", null, "mož", "čopek", "okno", "štor", "hiša", "parkirišče",
            "avto", "boben", "cvetje", "čistoča", "igra", "deblo", "zmaj"
    };

    @Test
    public void testDBWithIndexOpenClose() throws Exception {
        Database db = BiDB.newInstance("db/testdb");
        Store col1 = Stores.openStore(db,"table1");
        Store col2 = Stores.openStore(db,"table2");
        col1.clear();
        col2.clear();
        col1.addIndex("word");
        col2.addIndex("val");

        for (String word : words) {
            BiDocument doc = new BiDocument("word", word);
            UpdateResult<Long> res = col1.save(doc);
            doc = new BiDocument()
                    .add("val", word)
                    .add("word_id", res.getDocumentId());
            col2.save(doc);
        }

        col1.close();
        col2.close();

        col1 = Stores.openStore(db, "table1");
        Filter f = Filters.eq("val", "cvetje");
        Cursor cursor = col1.find(f);
        Log.p("{} records found: {}", cursor.size(), cursor.getFirst());
        col1.close();
    }

    @Test
    public void testSimpleSearch() throws Exception {
        Database db = BiDB.newInstance("db/testdb");
        Store col1 = Stores.openReadOnlyStore(db, "table1");
        printStoreAsTable(col1);

        Filter f = Filters.eq("val", "cvetje");
        Cursor cursor = col1.find(f);
        Log.p("{} records found: {}", cursor.size(), cursor.getFirst());
    }

    @Test
    public void testSloleksSearchNoIndex() throws Exception {
        Database db = BiDB.newInstance("db/sloleks");
        Store col1 = Stores.openReadOnlyStore(db, "nlp_lemma");
        // printStoreAsTable(col1);

        Filter f = Filters.eq("lemma", "cvetje");

        Cursor cursor = col1.find(f);

        Log.p("{} records found: {}", cursor.size(), cursor.getFirst());
    }

    @Test
    public void printStoreAsTable() throws Exception {
        Database db = BiDB.newInstance("db/testdb");
        Store store = Stores.openStore(db, "table1");

        int n = 0;
        for (String word : words) {
            BiDocument doc = new BiDocument()
                    .add("val", word)
                    .add("word", "some "+ word)
                    .add("num", ++n);
            store.save(doc);
        }

        printStoreAsTable(store);

        store.close();
    }

    public void printStoreAsTable(Store store) {
        TextUtils txt = new TextUtils();
        List<String> cols = new ArrayList<String>() {
            {add("ID");}
            {add("Value");}
            {add("Word");}
            {add("Num");}
        };

        List<List<String>> rows = new ArrayList<List<String>>();
        store.forEach(new Consumer<BiDocument>() {
            @Override
            public void accept(BiDocument doc) {
                List<String> row = new ArrayList<String>();
                row.add(doc.getId().toString());
                row.add(doc.getString("val"));
                row.add(doc.getString("word"));
                Integer intV = doc.getInteger("num");
                row.add(intV == null ? "" : intV.toString());
                rows.add(row);
            }
        });

        StringBuilder sb = txt.buildTable(cols, rows, 20);
        System.out.println(sb);
    }

    @Test
    public void openGigaDB() throws Exception {
        Database db = BiDB.newInstance("db/giga");
        Log.p("starting...");
        Stores.setStoreFactory(new LinkedHashMapStoreFactory());
        Store store = Stores.openStore(db, "mega");
        Log.p("store is opened");
        // store.close();
        Log.p("store is closed");
    }

    @Test
    public void testAllOperation() throws Exception {
        Database db = BiDB.newInstance("db/testdb");
        Store store = Stores.openStore(db, "table2");

        store.save(new BiDocument("word", "miza"));
        store.save(new BiDocument("word", "miza"));
        store.save(new BiDocument("word", "stol"));
        Log.p("Store size = {}", store.size());

        Filter f = Filters.eq("word", "miza");

        Log.p("Step #2");
        Cursor c = store.find(f);
        Log.p("Cursor size = {}", c.size());
        if (c.isEmpty()) {
            store.save(new BiDocument("word", "miza"));
        }
        Log.p("Store size = {}", store.size());

        Log.p("Step #3");
        c = store.find(f);
        Log.p("Cursor size = {}", c.size());
        if (c.isEmpty()) {
            store.save(new BiDocument("word", "miza"));
        }
        Log.p("Store size = {}", store.size());

    }

}
