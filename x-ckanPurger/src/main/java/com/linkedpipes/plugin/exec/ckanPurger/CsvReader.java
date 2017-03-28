package com.linkedpipes.plugin.exec.ckanPurger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

class CsvReader {

    private final static CsvPreference CSV_PREFERENCE
            = new CsvPreference.Builder('"', ',', "\\n").build();

    public static List<String> readColumn(File file, int columnIndex)
            throws IOException {
        try (final FileInputStream fileInputStream
                = new FileInputStream(file);
                final InputStreamReader inputStreamReader
                = new InputStreamReader(fileInputStream, "UTF-8");
                final CsvListReader csvReader
                = new CsvListReader(inputStreamReader, CSV_PREFERENCE)) {
            List<String> header = csvReader.read();
            return readColumn(csvReader, 0);
        }
    }

    private static List<String> readColumn(
            CsvListReader reader, int columnIndex) throws IOException {
        List<String> result = new LinkedList<>();
        List<String> row = reader.read();
        while (row != null) {
            result.add(row.get(columnIndex));
            row = reader.read();
        }
        return result;
    }

}
