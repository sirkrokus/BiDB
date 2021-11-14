package org.digga.bidb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BiDB {

    public static Database newInstance(Path path, DatabaseConfig config) {
        try {
            path = Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException("Impossible to create database on '"+path+"'");
        }
        return new Database(path);
    }

    public static Database newInstance(String location, DatabaseConfig config) {
        return newInstance(Paths.get(location), config);
    }

    public static Database newInstance(String location) {
        return newInstance(Paths.get(location), null);
    }

}
