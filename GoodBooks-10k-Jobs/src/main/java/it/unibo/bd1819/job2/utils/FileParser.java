package it.unibo.bd1819.job2.utils;

import java.util.Arrays;
import java.util.List;

public class FileParser {

    static public List<String> parseCSVLine(String line){
        return Arrays.asList(line.split(","));
    }
}
