package org.digga.bidb;

import org.digga.bidb.index.EHCacheIndex;
import org.digga.bidb.index.LongIndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

public abstract class InMemoryStore implements Store<Long> {

    private static Logger log = LoggerFactory.getLogger(InMemoryStore.class);

    private static final String FILE_EXT = ".dat";
    private static final String TMP_FILE_EXT = ".tmp";

    protected Database database;
    protected String storeName;
    protected long size = 0;
    protected LongIndexStore longIndexStore;
    protected boolean readonly = false;

    public InMemoryStore(Database database, String storeName, boolean readonly) {
        this.database = database;
        this.storeName = storeName;
        this.readonly = readonly;
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public boolean isReadonly() {
        return readonly;
    }

    protected void loadFromFile() throws Exception {
        Path dataPath = Paths.get(database.getLocation().toString(), storeName + FILE_EXT);
        File dataFile = dataPath.toFile();
        log.debug("Try to reload the store from " + dataFile);
        if (!dataFile.exists()) {
            log.info("Store '"+ storeName +"' doesn't exist");
            return;
        }

        size = 0;
        ObjectInputStream dataStream = new ObjectInputStream(new FileInputStream(dataFile));
        try {

            while (true) {
                //Timing.start("load");
                BiDocument doc = new BiDocument();
                doc.readExternal(dataStream);
                putDocument(doc.getId(), doc);
                size++;
                //Timing.stop("load", 50000);
            }

        } catch (NullPointerException e) {
            log.error("Data stream read error. " + e.getMessage(), e);
        } catch (ClassCastException e) {
            log.error("Data stream read error. " + e.getMessage(), e);
        } catch (Exception e) {
            log.info("Data stream is ended. " + e.getClass());
        }

        dataStream.close();
        log.info(size + " document(s) are loaded");

        File dbFolder = database.getLocation().toFile();
        File[] idxFiles = dbFolder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(storeName + "_") && name.endsWith(EHCacheIndex.IDX_EXT);
            }
        });

        for (File idxFile : idxFiles) {
            int idx1 = idxFile.getName().lastIndexOf("_");
            int idx2 = idxFile.getName().lastIndexOf(EHCacheIndex.IDX_EXT);
            String idxName = idxFile.getName().substring(idx1 + 1, idx2);
            if (longIndexStore.hasIndex(idxName)) {
                longIndexStore.getIndex(idxName).open();
            } else {
                addIndex(idxName);
            }
        }

    }

    protected void saveStoreToFile() throws IOException {
        if (readonly) {
            throw new IOException("Store is opened in readonly mode");
        }

        Path tmpPath = Paths.get(database.getLocation().toString(), storeName + TMP_FILE_EXT);
        File tmpFile = tmpPath.toFile();
        log.debug("Try to save the store to " + tmpFile);

        ObjectOutputStream dataStream = new ObjectOutputStream(new FileOutputStream(tmpFile));
        int docCount = forEach(new Consumer<BiDocument>() {
            @Override
            public void accept(BiDocument doc) {
                //Timing.start("save");
                try {
                    doc.writeExternal(dataStream);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //Timing.stop("save", 10000);
            }
        });
        dataStream.close();
        log.info(docCount + " document(s) are saved");

        Path dataPath = Paths.get(database.getLocation().toString(),storeName + FILE_EXT);
        File dataFile = dataPath.toFile();
        if (dataFile.exists()) {
            dataFile.delete();
        }
        boolean r = tmpFile.renameTo(dataFile);
        if (!r) {
            log.error("Impossible to rename a temporary file " + tmpFile + ". Data is not saved");
        }
    }

    @Override
    public long size() {
        return size;
    }

}
