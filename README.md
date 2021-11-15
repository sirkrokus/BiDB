# BiDB

Simple in-memory document oriented database with a MongoDB like query language. 
```
Database db = BiDB.newInstance("db/small"); // creates new directory where collections will be stored
Store col = Stores.openStore(db, "words"); // open a new collection

// add some documents
col.save(new BiDocument("word", "table"));
col.save(new BiDocument("word", "window"));
col.save(new BiDocument("word", "car"));

// add index (optionally)
col.addIndex("word");

// try to find it
Filter f = Filters.or(
    Filters.eq("word", "table"),
    Filters.eq("word", "car")
);

Cursor c = col.find(f);
c.forEach(doc -> System.out.println(doc));
```
