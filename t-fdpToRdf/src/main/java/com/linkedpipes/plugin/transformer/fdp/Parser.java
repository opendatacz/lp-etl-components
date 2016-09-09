package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.dataunit.system.api.files.FilesDataUnit;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.QuoteMode;
import org.supercsv.util.CsvContext;

/**
 *
 * @author Petr Škoda
 */
class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

    private final ExceptionFactory exceptionFactory;
    
    private final CsvPreference csvPreference;

    Parser(ExceptionFactory exceptionFactory) {
            csvPreference = new CsvPreference.Builder(
            		'\"',
            		',',
                    "\\n").build();
            this.exceptionFactory = exceptionFactory;
    }

    public void parse(FilesDataUnit.Entry entry, Mapper mapper)
            throws UnsupportedEncodingException, IOException, LpException {
        try (final FileInputStream fileInputStream
                = new FileInputStream(entry.toFile());
                final InputStreamReader inputStreamReader
                = getInputStream(fileInputStream);
                final BufferedReader bufferedReader
                = new BufferedReader(inputStreamReader);
                final CsvListReader csvListReader
                = new CsvListReader(bufferedReader, csvPreference)) {
            List<String> header;
            List<String> row;

                header = Arrays.asList(csvListReader.getHeader(true));
                row = csvListReader.read();

            try {
                mapper.onHeader(header);
            } catch (Exception ex) {
                throw exceptionFactory.failure("Can initalize on header row.",
                        ex);
            }
            if (row == null) {
                LOG.info("No data found in file: {}", entry.getFileName());
                return;
            }
            while (row != null) {
                if (!mapper.onRow(row)) {
                    break;
                }
                row = csvListReader.read();
            }
        }
    }

    private static List<String> trimList(List<String> row) {
        final List<String> trimmedRow = new ArrayList<>(row.size());
        for (String item : row) {
            if (item != null) {
                item = item.trim();
            }
            trimmedRow.add(item);
        }
        return trimmedRow;
    }

    /**
     * Create {@link InputStreamReader}. If "UTF-8" as encoding is given then
     * {@link BOMInputStream} is used as wrap of given fileInputStream
     * and output {@link InputStreamReader} to remove possible
     * BOM mark at the start of "UTF" files.
     *
     * @param fileInputStream
     * @return
     * @throws UnsupportedEncodingException
     */
    private InputStreamReader getInputStream(FileInputStream fileInputStream)
            throws UnsupportedEncodingException {
    	
        return new InputStreamReader(
                    new BOMInputStream(fileInputStream, false),
                    "UTF-8");
    }

}
