package com.linkedpipes.plugin.transformer.fdp;
import org.supercsv.prefs.CsvPreference;

/**
 * Created by admin on 5.12.2016.
 */
public class HeaderParser {
    public static final String resourceQuery(String resourceName) {
        return "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>" +
                "PREFIX schema: <http://schema.org/>\n" +
                "PREFIX qb: <http://purl.org/linked-data/cube#>\r\n" +
                "PREFIX fdp: <http://schemas.frictionlessdata.io/fiscal-data-package#>" +
                "SELECT * WHERE {" +
                "?resource fdp:name ?name;" +
                "          fdp:schema ?schema ." +
                "FILTER(?name = \""+resourceName+"\")" +
                "OPTIONAL {" +
                "   ?resource fdp:dialect [" +
                "       fdp:delimiter ?delimiter ] .}" +
                "OPTIONAL {" +
                "   ?resource fdp:dialect [" +
                "       fdp:quoteChar ?quoteChar ] .}" +
                "}";
    }
    public static final String fieldsQuery =
            "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>" +
                    "PREFIX schema: <http://schema.org/>\n" +
                    "PREFIX qb: <http://purl.org/linked-data/cube#>\r\n" +
                    "PREFIX fdp: <http://schemas.frictionlessdata.io/fiscal-data-package#>" +
                    "SELECT * WHERE {" +
                    "_schema_ fdp:fields ?field." +
                    "?field fdp:name ?fieldName;" +
                    "       fdp:decimalChar ?decimalChar ." +
                    "OPTIONAL {?field fdp:groupChar ?groupChar .}" +
                    "} ";
    private int delimiter;
    private char quoteChar;

    public HeaderParser() {
        delimiter = ',';
        quoteChar = '\"';
    }
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter.charAt(0);
    }
    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar.charAt(0);
    }
    public CsvPreference getCsvPreference() {
        CsvPreference csvPreference = new CsvPreference.Builder(
                quoteChar,
                delimiter,
                "\\n").build();
        return csvPreference;
    }


}
