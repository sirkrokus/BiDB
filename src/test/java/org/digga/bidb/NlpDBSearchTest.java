package org.digga.bidb;


import org.digga.bidb.filter.Filter;
import org.digga.bidb.filter.Filters;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Consumer;

public class NlpDBSearchTest {

    // F-4, M-5, N-5
    private String[] nouns = new String[] {
            "mizi;NOUN;PREP;F",
            "moža;NOUN;GEN;M",
            "čopek;NOUN;NOM;M",
            "okno;NOUN;NOM;N",
            "štoru;NOUN;NOM;LOC;M",
            "hiša;NOUN;NOM;F",
            "parkirišče;NOUN;NOM;N",
            "avto;NOUN;NOM;N",
            "bobnom;NOUN;INST;M",
            "cvetje;NOUN;NOM;N",
            "čistoča;NOUN;NOM;F",
            "igra;NOUN;NOM;F",
            "deblo;NOUN;NOM;N",
            "zmaj;NOUN;NOM;M"
    };

    public NlpDBSearchTest() {
    }

    @Test
    // search in nonindexed db
    public void testSimpleSearch() throws IOException {
        Database db = BiDB.newInstance("db/small");
        Store col = Stores.openStore(db, "words");
        col.clear();
        col.save(new BiDocument("word", "miza"));
        col.save(new BiDocument("word", "okno"));
        col.save(new BiDocument("word", "miza"));
        col.save(new BiDocument("word", "stol"));
        col.save(new BiDocument("word", "knjiga"));
        col.save(new BiDocument("word", "avto"));
        Log.p("Collection size: {}", col.size());

        Filter f = Filters.or(
                Filters.eq("word", "miza"),
                Filters.eq("word", "okno")
        );
        Cursor c = col.find(f);
        Log.p("--- Search result: {} ---", c.size());
        c.forEach((Consumer<BiDocument>) doc -> Log.p("{}", doc));
    }

    @Test
    public void testIndexedSearch() throws Exception {
        Store col = createDBIndexedNouns();

        /*
        Log.p("------- by word -------");
        Filter f = Filters.eq("word", "okno");
        Cursor c = col.find(f);
        c.forEach(doc -> Log.p("{}", doc.asString(false)));

        Log.p("------- by gender -------");
        f = Filters.eq("gender", "F");
        c = col.find(f);
        c.forEach(doc -> Log.p("{}", doc.asString(false)));
        */

        Filter f = Filters.and()
                .eq("case", "GEN")
                .eq("gender", "M");
        Cursor c = col.find(f);
        Log.p("------ AND: {} --------", c.size());
        c.forEach(doc -> Log.p("{}", doc.asString(false)));

        f = Filters.or()
                .eq("gender", "F")
                .eq("gender", "M");
        c = col.find(f);
        Log.p("------- OR: {} -------", c.size());
        c.forEach(doc -> Log.p("{}", doc.asString(false)));

        if (true) {
            col.close();
            return;
        }

    }

    @Test
    public void testCreateDBGiga() throws Exception {
        Store col = createDBGiga();
        //col.addIndex("word");
        col.close();

/*
        for (int i = 0; i < 3; i++) {
            long t1 = System.currentTimeMillis();
            for (int j = 0; j < 10; j++) {
                Filter f = Filters.eq("word", "word999999");
                Cursor c = col.find(f);
                c.forEach((Consumer<Document>) doc -> doc.asString(false));
            }
            long t2 = System.currentTimeMillis();
            Log.p("Time: {} ms", (t2-t1));
        }
*/

    }

    @Test
    public void testSpeedIndexedNonIndexed() throws Exception {
        Store col = createDBMega();

        Filter f = Filters.eq("word", "word9999");
        measureSearchSpeed(col, f);

        Log.p("-------- with index ----------");
        col.addIndex("word");
        measureSearchSpeed(col, f);

        /*
        EHCacheStore
        [18:28:31] MegaDB. Collection size 100000
        [18:28:39] Time 8439 ms
        [18:28:47] Time 7871 ms
        [18:28:56] Time 8315 ms
        [18:28:56] -------- with index ----------
        [18:28:56] Time 4 ms
        [18:28:56] Time 3 ms
        [18:28:56] Time 2 ms

        LinkedHashMapStore
        [18:30:22] Time 3031 ms
        [18:30:23] Time 1674 ms
        [18:30:25] Time 1684 ms
        [18:30:25] -------- with index ----------
        [18:30:27] Time 10 ms
        [18:30:27] Time 5 ms
        [18:30:27] Time 9 ms
        * */
    }

    private void measureSearchSpeed(Store store, Filter f) throws IOException {
        for (int r = 0; r < 3; r++) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                Cursor c = store.find(f);
                c.getFirst();
            }
            long t2 = System.currentTimeMillis();
            Log.p("Time {} ms", (t2 - t1));
        }
    }

    // ========================== TEST DATABASES =====================

    private Store createDBGiga() throws IOException {
        Database db = BiDB.newInstance("db/giga");
        Store col = Stores.openStore(db, "mega");
        col.clear();

        for (int i = 0; i < 1000000; i++) {
            BiDocument doc = new BiDocument()
                    .add("num", i)
                    .add("pos", "NOUN")
                    .add("feature", "FEMALE")
                    .add("word", "word"+i);
            col.save(doc);
        }
        Log.p("GigaDB. Collection size {}", col.size());

        return col;
    }

    private Store createDBMega() throws IOException {
        Database db = BiDB.newInstance("db/mega");
        Stores.setStoreFactory(new LinkedHashMapStoreFactory());
        Store col = Stores.openStore(db, "store");
        col.clear();

        for (int i = 0; i < 100000; i++) {
            BiDocument doc = new BiDocument()
                    .add("word", "word"+i);
            col.save(doc);
        }
        Log.p("MegaDB. Collection size {}", col.size());

        return col;
    }

    private Store createDBNouns() throws IOException {
        Database db = BiDB.newInstance("db/nouns");
        Store col = Stores.openStore(db, "nouns");
        col.clear();
        addNouns(col);
        Log.p("NounsDB. Collection size {}", col.size());

        return col;
    }

    private Store createDBIndexedNouns() throws Exception {
        Database db = BiDB.newInstance("db/indexednouns");
        Store col = Stores.openStore(db, "nouns");
        if (col.size() == 0) {
            col.clear();
            addNouns(col);
        }
        col.addIndex("word");
        col.addIndex("gender");
        col.addIndex("pos");
        col.addIndex("case");

        Log.p("IndexedNounsDB. Collection size {}", col.size());

        return col;
    }

    private void addNouns(Store col) throws IOException {
        for (String noun : nouns) {
            String[] toks = noun.split(";");
            BiDocument doc = new BiDocument()
                    .add("word", toks[0])
                    .add("pos", toks[1])
                    .add("case", toks[2])
                    .add("gender", toks[3]);
            col.save(doc);
        }
    }


}
