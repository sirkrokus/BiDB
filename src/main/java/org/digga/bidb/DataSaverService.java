package org.digga.bidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DataSaverService {

    private static Logger log = LoggerFactory.getLogger(DataSaverService.class);

    private static final String FILE_EXT = ".dat";
    private static final String TMP_FILE_EXT = ".tmp";

    private File tmpFile;
    private ObjectOutputStream dataStream;
    private int docCount = 0;

    protected Database database;
    protected String storeName;

    public DataSaverService(Database database, String storeName) {
        this.database = database;
        this.storeName = storeName;
    }

    public void openStream() throws Exception {
        Path tmpPath = Paths.get(database.getLocation().toString(), storeName + TMP_FILE_EXT);
        tmpFile = tmpPath.toFile();
        log.debug("Try to save the store to " + tmpFile);

        dataStream = new ObjectOutputStream(new FileOutputStream(tmpFile));
        docCount = 0;
    }

    public void saveToStream(DataExternalizable data) throws IOException {
        data.writeExternal(dataStream);
        docCount++;
    }

    public void closeStream() throws Exception {
        dataStream.close();
        log.info(docCount + " document(s) are saved");

        Path dataPath = Paths.get(database.getLocation().toString(), storeName + FILE_EXT);
        File dataFile = dataPath.toFile();
        if (dataFile.exists()) {
            dataFile.delete();
        }
        boolean r = tmpFile.renameTo(dataFile);
        if (!r) {
            log.error("Impossible to rename a temporary file " + tmpFile + ". Data is not saved");
        }
    }

/*
    protected int loadFromFile(BiDocumentCollection<Long> collection) throws Exception {
        Path dataPath = Paths.get(database.getLocation().toString(), storeName + FILE_EXT);
        File dataFile = dataPath.toFile();
        log.debug("Try to reload the store from " + dataFile);
        if (!dataFile.exists()) {
            throw new Exception("Store '"+ storeName +"' doesn't exist");
        }

        int size = 0;
        ObjectInputStream dataStream = new ObjectInputStream(new FileInputStream(dataFile));
        try {

            while (true) {
                //Timing.start("load");
                BiDocument doc = new BiDocument();
                doc.readExternal(dataStream);
                collection.putDocument(doc.getId(), doc);
                size++;
                //Timing.stop("load", 50000);
            }

        } catch (Exception e) {
            log.info("Data stream is ended");
        }

        dataStream.close();
        log.info(size + " document(s) are loaded");

        return size;
    }

    protected void save(BiDocumentCollection<Long> collection) throws IOException {

        Path tmpPath = Paths.get(database.getLocation().toString(), storeName + TMP_FILE_EXT);
        File tmpFile = tmpPath.toFile();
        log.debug("Try to save the store to " + tmpFile);

        ObjectOutputStream dataStream = new ObjectOutputStream(new FileOutputStream(tmpFile));
        int docCount = collection.forEach(new Consumer<BiDocument>() {
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

        Path dataPath = Paths.get(database.getLocation().toString(), storeName + FILE_EXT);
        File dataFile = dataPath.toFile();
        if (dataFile.exists()) {
            dataFile.delete();
        }
        boolean r = tmpFile.renameTo(dataFile);
        if (!r) {
            log.error("Impossible to rename a temporary file " + tmpFile + ". Data is not saved");
        }
    }
*/

}
