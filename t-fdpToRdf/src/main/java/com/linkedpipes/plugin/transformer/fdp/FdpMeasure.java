package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;
import org.openrdf.model.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

/**
 * Created by admin on 21.8.2016.
 */
public class FdpMeasure {
    private double factor;
    private String measureProperty;
    private String column;
    private String file;
    private PlainTextTripleWriter output;
    private FdpAttribute attr;

    public static final String query =
            "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
            "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\n" +
            "\n" +
            "SELECT *\n" +
            "WHERE {\n" +
            "  ?dsd a qb:DataStructureDefinition;\n" +
            "       qb:component ?component ." +
            "       ?component qb:measure ?measureProperty;\n" +
            "                      fdprdf:source ?measureSource; \n" +
            "                      fdprdf:factor ?measureFactor;\n" +
            "  \t\t\t\t\t  fdprdf:sourceColumn ?sourceColumn;\n" +
            "  \t\t\t\t\t  fdprdf:sourceFile ?sourceFile .\n" +
            "                        \n" +
            "  ?dataset a qb:DataSet;\n" +
            "      fdprdf:datasetShortName ?packageName ;\n" +
            "      \t   qb:structure ?dsd .\n" +
                    "" +
            "   OPTIONAL { ?component fdprdf:decimalChar ?decimalChar . }\n" +
                    " OPTIONAL { ?component fdprdf:groupChar ?groupChar . }\n" +
                    " OPTIONAL { ?component fdprdf:fieldType ?fieldType . }" +
            "}";

    public FdpMeasure(PlainTextTripleWriter output, double factor, String measureProperty, String column, String file) {
        this.factor = factor;
        this.measureProperty = measureProperty;
        this.column = column;
        this.file = file;
        this.output = output;
        attr = new FdpAttribute(column, file, false, Mapper.VALUE_FACTORY.createIRI(measureProperty));
        this.attr.setFormat(FdpAttribute.Format.NUMBER);
    }

    public void setDecimalSep(char sep) {
        this.attr.setDecimalSep(sep);
    }

    public void setGroupSep(char sep) {
        this.attr.setGroupSep(sep);
    }

    public void processRow(IRI observationIri, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException, IOException {
        String measureValString = row.get(column);
        if(measureValString!=null) {
            Double numericVal = (Double) attr.parseValue(measureValString);
            if (numericVal != null) {
                BigDecimal measureVal = new BigDecimal(numericVal * this.factor);
                BigDecimal roundedVal = measureVal.setScale(2, RoundingMode.HALF_UP);
                output.submit(observationIri,
                        Mapper.VALUE_FACTORY.createIRI(measureProperty),
                        Mapper.VALUE_FACTORY.createLiteral(roundedVal));
            } else {
                output.submit(observationIri,
                        Mapper.VALUE_FACTORY.createIRI(measureProperty),
                        Mapper.VALUE_FACTORY.createLiteral(measureValString));
            }
        }
    }
}
