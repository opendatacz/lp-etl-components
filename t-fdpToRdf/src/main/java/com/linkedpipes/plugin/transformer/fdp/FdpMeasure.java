package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;
import org.openrdf.model.*;

import java.util.HashMap;

/**
 * Created by admin on 21.8.2016.
 */
public class FdpMeasure {
    private double factor;
    private String measureProperty;
    private String column;
    private String file;
    private StatementConsumer output;

    public static final String query =
            "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
            "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\n" +
            "\n" +
            "SELECT *\n" +
            "WHERE {\n" +
            "  ?dsd a qb:DataStructureDefinition;\n" +
            "       qb:component [ qb:measure ?measureProperty;\n" +
            "                      fdprdf:source ?measureSource; \n" +
            "                      fdprdf:factor ?measureFactor;\n" +
            "  \t\t\t\t\t  fdprdf:sourceColumn ?sourceColumn;\n" +
            "  \t\t\t\t\t  fdprdf:sourceFile ?sourceFile ] .\n" +
            "                        \n" +
            "  ?dataset a qb:DataSet;\n" +
            "      \t   qb:structure ?dsd .\n" +
            "}";

    public FdpMeasure(StatementConsumer output, double factor, String measureProperty, String column, String file) {
        this.factor = factor;
        this.measureProperty = measureProperty;
        this.column = column;
        this.file = file;
        this.output = output;
    }

    public void processRow(IRI observationIri, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException {
        String measureValString = row.get(column);
        if(measureValString != null) {
            try {
                double measureVal = Double.parseDouble(measureValString) * this.factor;
                output.submit(observationIri,
                        Mapper.VALUE_FACTORY.createIRI(measureProperty),
                        Mapper.VALUE_FACTORY.createLiteral(measureVal));
            }
            catch(NumberFormatException nfe) {
                output.submit(observationIri,
                        Mapper.VALUE_FACTORY.createIRI(measureProperty),
                        Mapper.VALUE_FACTORY.createLiteral(measureValString));
            }
        }
    }
}
