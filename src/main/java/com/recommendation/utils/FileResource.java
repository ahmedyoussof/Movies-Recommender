package com.recommendation.utils;


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class FileResource {
    String fileName;
    public FileResource(String filename) {
        this.fileName = filename;
    }

    public CSVParser getCSVParser() {
        try {
            Path p  =  Paths.get(fileName);
            BufferedReader reader = Files.newBufferedReader(p);
            return CSVFormat.DEFAULT.withHeader().parse(reader);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}

