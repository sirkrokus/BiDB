package org.digga.bidb;

import org.digga.bidb.filter.*;
import org.digga.bidb.index.IndexStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class NlpDBFilterTest {

    @Test
    public void testIntesect() {
        Set<Long> set1 = new HashSet<>();
        Set<Long> set2 = new HashSet<>();
        Set<Long> set3 = new HashSet<>();

        for (int i = 100; i < 2000000; i++) {
            set1.add(new Long(i));
        }

        for (int i = 999000; i < 2000000; i++) {
            set2.add(new Long(i));
        }

        for (int i = 999100; i < 999900; i++) {
            set3.add(new Long(i));
        }

        System.out.println("Size 1=" + set2.size());
        set2.retainAll(set1);
        System.out.println("Size 2=" + set2.size());
        set2.retainAll(set3);
        System.out.println("Size 3=" + set2.size());

    }

    @Test
    public void testFilter2() {
        Filter or1 = Filters.and()
                .eq("name", "ivan");

        Filter or21 = Filters.and()
                .eq("type", "long");
        Filter or2 = Filters.and()
                .eq("name", "petr")
                .or(or21);

        Filter query = Filters.and()
                .eq("pos", "NOUN")
                .eq("case", "DAT")
                .or(or1, or2);
/*
        query.forEach(new Consumer<FieldFilter>() {
            @Override
            public void accept(FieldFilter f) {
                System.out.println("FieldName=" + f.getFieldName());
            }
        });*/
    }

    @Test
    public void testFilters() {
        List<BiDocument> doclist = new ArrayList<>();
        BiDocument doc1 = new BiDocument()
                .add("word", "okno")
                .add("pos", "NOUN")
                .add("case", "NOM");
        BiDocument doc2 = new BiDocument()
                .add("word", "miza")
                .add("pos", "NOUN")
                .add("case", "DAT");
        BiDocument doc3 = new BiDocument()
                .add("word", "iti")
                .add("pos", "VERB")
                .add("case", "DAT");
        BiDocument doc4 = new BiDocument()
                .add("word", "stol")
                .add("pos", "NOUN")
                .add("case", "DAT");
        doclist.add(doc1);
        doclist.add(doc2);
        doclist.add(doc3);
        doclist.add(doc4);

        // 1.
        System.out.println("----------------------------------");
        Filter query = Filters.and().eq("word", "okno");
        int count = 0;
        for (BiDocument BiDocument : doclist) {
            boolean m = true;// query.match(BiDocument);
            if (m) {
                System.out.println(BiDocument.asString(false));
                count++;
            }
        }
        Assert.assertEquals("case #1",1, count);

        // 2.
        System.out.println("----------------------------------");
        query = Filters.and()
                .eq("pos", "NOUN")
                .eq("case", "DAT");
        count = 0;
        for (BiDocument BiDocument : doclist) {
            boolean m = true;//query.match(BiDocument);
            if (m) {
                System.out.println(BiDocument.asString(false));
                count++;
            }
        }
        Assert.assertEquals("case #2",2, count);

        // 3.
        System.out.println("----------------------------------");
        Filter f1 = Filters.and().eq("pos", "VERB");
        Filter f2 = Filters.and().eq("word", "stol");
        query = Filters.or(f1, f2);
        count = 0;
        for (BiDocument BiDocument : doclist) {
            boolean m = true;//query.match(BiDocument);
            if (m) {
                System.out.println(BiDocument.asString(false));
                count++;
            }
        }
        Assert.assertEquals("case #3",2, count);

    }

    @Test
    public void testFiltersHash() {
        EqualsFilter f1 = new EqualsFilter("pos", "NOUN");
        NotEqualsFilter f2 = new NotEqualsFilter("pos", "NOUN");
        Log.p("Hash1={}, Hash2={}", f1.hashCode(), f2.hashCode());
    }

    @Test
    public void testEqualsNotEquals() {
        BiDocument doc = new BiDocument("pos", "noun");

        EqualsFilter eq1 = new EqualsFilter("pos", "noun");
        boolean m = true;// eq1.match(doc);
        Log.p("EQ match {}", m);

        eq1 = new EqualsFilter("pos", null);
        //m = eq1.match(doc);
        Log.p("EQ match {}", m);

        NotEqualsFilter ne1 = new NotEqualsFilter("pos", "noun");
        //m = ne1.match(doc);
        Log.p("NE match {}", m);

        ne1 = new NotEqualsFilter("pos", null);
        //m = ne1.match(doc);
        Log.p("NE match {}", m);
    }

    @Test
    public void testLeafs() {
        Filter or1 = Filters.ne("o1","3");
        Filter or2 = Filters.ne("o1","4");
        Filter or3 = Filters.and()
                .eq("o1", "3")
                .eq("f2", "3");
        Filter or4 = Filters.and()
                .eq("o1", "5")
                .eq("f2", "2");
        Filter f = Filters.and()
                .eq("f1", "1")
                .eq("f2", "2")
                .or(or1, or2)
                .or(or3, or4);

        // List<FieldFilter> leafs = new ArrayList<>();
        Set<FieldFilter> leafs = new HashSet<>();
        new FilterHandler(f).collectLeafsTo(leafs);
        for (FieldFilter leaf : leafs) {
            Log.p("Leaf: {}", leaf);
        }
    }

    @Test
    public void searchTest() {
        Filter f = Filters.and()
                .eq("word", "okno")
                .eq("pos", "NOUN");

        Map<FieldFilter, Set<Long>> nonIndexedFieldMap = new HashMap<>();
        Map<FieldFilter, Set<Long>> indexedFieldMap = new HashMap<>();

        // ищем какие поля индексированы какие нет
        new FilterHandler(f).handleLeafs(leaf -> {
            Log.p("Leaf: {}", leaf);

            if (leaf.getFieldName().equals("word")) {
                indexedFieldMap.put(leaf, new HashSet<>());
            } else {
                nonIndexedFieldMap.put(leaf, new HashSet<>());
            }
        });

        Log.p("indexed {}", indexedFieldMap);
        Log.p("nonindexed {}", nonIndexedFieldMap);
/*
        // для неиндексированных полей пройти через всю коллекцию
        if (!nonIndexedFieldMap.isEmpty()) {
            collection.forEach(doc -> {
                for (FieldFilter fieldFilter : nonIndexedFieldMap.keySet()) {
                    if (fieldFilter.match(doc)) {
                        nonIndexedFieldMap.get(fieldFilter).put(doc.getId());
                    }
                }
            });
        }

        // для индексированных взять данные из индекса
        IndexService<Long> idxService = new IndexService(null, "", null);
        if (!indexedFieldMap.isEmpty()) {
            for (FieldFilter fieldFilter : indexedFieldMap.keySet()) {
                Long[] ids = fieldFilter.getPointers(idxService);
                indexedFieldMap.get(fieldFilter).putAll(ids);
            }
        }
*/

    }

    @Test
    public void testTraverse() {
        Filter or1 = Filters.ne("o1","3");
        Filter or2 = Filters.ne("o1","4");
        Filter or3 = Filters.and()
                .eq("o1", "3")
                .eq("f2", "3");
        Filter or4 = Filters.and()
                .eq("o1", "5")
                .eq("f2", "2");
        Filter f = Filters.and()
                .eq("f1", "1")
                .eq("f2", "2")
                .or(or1, or2)
                .or(or3, or4);

        IndexStore indexStore = null;

        new FilterHandler(f).traverse(filter -> {
            // filter.apply(indexStore);
        });
    }

}
