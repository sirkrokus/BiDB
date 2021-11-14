package org.digga.bidb;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.*;

public class IndexResearch {

    // индексация одного поля NAME

    private Map<Long, Rec> db = new HashMap<>();
    private Map<String, LenBasedIndex> lenBasedIndexer = new HashMap<>();

    @Test
    public void test() {
        putToDB(new Rec(1, "na", 12));
        putToDB(new Rec(2, "na", 13));
        putToDB(new Rec(3, "nana", 13));
        putToDB(new Rec(4, "nanu", 13));
        putToDB(new Rec(5, "nanaza", 16));
        putToDB(new Rec(6, "nanada", 16));
        putToDB(new Rec(7, "nanadapa", 21));
        putToDB(new Rec(8, "mi", 21));
        putToDB(new Rec(9, "mim", 22));
        putToDB(new Rec(10, "mim", 99));
        putToDB(new Rec(11, "mimi", 23));
        putToDB(new Rec(12, "mimika", 23));
        putToDB(new Rec(13, "mimikada", 26));
        putToDB(new Rec(14, "mimikadu", 26));
        putToDB(new Rec(15, "mimikatu", 33));

        reindex();

        for (String desc : lenBasedIndexer.keySet()) {
            LenBasedIndex lenIdx = lenBasedIndexer.get(desc);
            Log.p("--[{}]--", desc);
            for (String indexedValue : lenIdx.idMap.keySet()) {
                Log.str("{}: ", indexedValue);
                Set<Long> ids = lenIdx.idMap.get(indexedValue);
                for (Long id : ids) {
                    Log.str("{}, ", id);
                }
                Log.ln();
            }
        }

        Log.p("na => {}", find("na"));
        Log.p("mimikadu => {}", find("mimikadu"));
    }

    public void putToDB(Rec rec) {
        db.put(rec.id, rec);
    }

    private void reindex() {
        db.forEach((id, rec) -> {
            String val = rec.name;
            if (rec.name.length() > 5) {
                val = val.substring(0, 5);
            }
            LenBasedIndex lbIndex = getLenBasedIndexer(val);
            lbIndex.addId(rec.name, id);
        });
    }

    private LenBasedIndex getLenBasedIndexer(String rangeDescriptor) {
        LenBasedIndex lbIndex = lenBasedIndexer.get(rangeDescriptor);
        if (lbIndex == null) {
            lbIndex = new LenBasedIndex(rangeDescriptor);
            lenBasedIndexer.put(rangeDescriptor, lbIndex);
        }
        return lbIndex;
    }

    private List<Rec> find(String value) {
        String range = value;
        if (range.length() > 5) {
            range = range.substring(0, 5);
        }
        LenBasedIndex lbIndex = getLenBasedIndexer(range);
        return lbIndex.findIndices(value);
    }

    class Rec {
        public Long id;
        public String name;
        public int age;

        public Rec() {
        }

        public Rec(int id, String name, int age) {
            this.id = (long)id;
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Rec [" + id + "]. name='" + name + '\'' +
                    ", age=" + age;
        }
    }

    class LenBasedIndex {
        public String rangeDescriptor;
        public Map<String, Set<Long>> idMap = new HashMap<>();
        // в файле на диске, отсортированный по алфавиту
        // длина для здначения фиксирована, на поле 3 файла - значения, ID-шники, индекс на значения для быстрого поиска

        public LenBasedIndex(String rangeDescriptor) {
            this.rangeDescriptor = rangeDescriptor;
        }

        public void addId(String indexedValue, Long id) {
            Set<Long> idSet = idMap.get(indexedValue);
            if (idSet == null) {
                idSet = new HashSet<>();
                idMap.put(indexedValue, idSet);
            }
            idSet.add(id);
        }

        public List<Rec> findIndices(String value) {
            Set<Long> idSet = idMap.get(value);
            if (idSet == null) {
                return Collections.emptyList();
            }
            List<Rec> recs = new ArrayList<Rec>();
            for (Long id : idSet) {
                recs.add(db.get(id));
            }
            return recs;
        }

    }

    class LenFileBasedIndex {
        public String collectionName;
        public String indexName;
        // public Map<String, Set<Long>> idMap = new HashMap<>();
        private RandomAccessFile randomFileValues;
        private RandomAccessFile randomFileIds;
        private RandomAccessFile randomFile;

        // в файле на диске, отсортированный по алфавиту
        // длина для значения фиксирована, на поле 3 файла - значения, ID-шники, индекс на значения для быстрого поиска

        public LenFileBasedIndex(String collectionName, String indexName) {
            this.collectionName = collectionName;
            this.indexName = indexName;
            try {
                randomFileValues = new RandomAccessFile(collectionName + "_" + indexName + ".val", "rw");
                randomFileIds = new RandomAccessFile(collectionName + "_" + indexName + ".ids", "rw");
                randomFile = new RandomAccessFile(collectionName + "_" + indexName + ".idx", "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
/*

        public void addId(String indexedValue, Long id) {
            Set<Long> idSet = idMap.get(indexedValue);
            if (idSet == null) {
                idSet = new HashSet<>();
                idMap.put(indexedValue, idSet);
            }
            idSet.add(id);
        }

        public List<Rec> findIndices(String value) {
            Set<Long> idSet = idMap.get(value);
            if (idSet == null) {
                return Collections.emptyList();
            }
            List<Rec> recs = new ArrayList<Rec>();
            for (Long id : idSet) {
                recs.add(db.get(id));
            }
            return recs;
        }
*/

    }

}
