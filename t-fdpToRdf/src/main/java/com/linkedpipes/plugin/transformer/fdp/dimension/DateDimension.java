package com.linkedpipes.plugin.transformer.fdp.dimension;

import com.linkedpipes.etl.executor.api.v1.LpException;
import com.linkedpipes.etl.executor.api.v1.service.ExceptionFactory;
import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.FdpToRdfVocabulary;
import com.linkedpipes.plugin.transformer.fdp.Mapper;

import java.io.IOException;
import java.util.HashMap;
import org.eclipse.rdf4j.model.IRI;

/**
 * Created by admin on 24.8.2016.
 */
public class DateDimension extends FdpDimension {
    public static final String attributeQuery =
            "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>" +
                    "PREFIX schema: <http://schema.org/>\n" +
                    "PREFIX qb: <http://purl.org/linked-data/cube#>\r\n" +
                    "\n" +
                    "SELECT *\n" +
                    "WHERE { \n" +
                    "  ?component ?componentProperty _dimensionProp_;\r\n" +
                    "             fdprdf:attribute ?attribute .\r\n" +
                    "             \r\n" +
                    "  \r\n" +
                    "  ?attribute fdprdf:sourceColumn ?sourceColumn ;\r\n"
                    + "			  fdprdf:sourceFile ?sourceFile;"
                    + "			  fdprdf:iskey ?iskey;" +
                    "             fdprdf:valueProperty ?attributeValueProperty .\r\n" +
                    "                        \r\n" +
                    "  ?dataset a qb:DataSet;  \r\n" +
                    "      	   qb:structure ?dsd .           \r\n" +
                    "  \r\n" +
                    "}";

    public String getAttributeQueryTemplate() {
        return this.attributeQuery;
    }

    public static final String dimensionQuery =
            "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
                    "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>" +
                    "PREFIX schema: <http://schema.org/>\n" +
                    "\n" +
                    "SELECT *\n" +
                    "WHERE { \n" +
                    "  ?component fdprdf:attributeCount ?attrCount .\n" +
                    "  FILTER(?attrCount = 1)\n" +
                    "  ?dsd a qb:DataStructureDefinition ;\n" +
                    "         qb:component ?component .\n" +
                    "  ?component qb:dimension ?dimensionProp;\n" +
                    "             fdprdf:valueType fdprdf:dateTime .\n" +
                    "                          \n" +
                    "   ?dataset a qb:DataSet;" +
                    "      fdprdf:datasetShortName ?packageName ;\n" +
                    "      qb:structure ?dsd ." +
                    "  ?dimensionProp fdprdf:name ?dimensionName .\r\n" +
                    "}  ";

    public DateDimension(){}

    public void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException, IOException {
        for(FdpAttribute attr : attributes) {
            String attrVal = row.get(attr.getColumn());
            String dateUri = null;
            if(attrVal != null) {
                if(attrVal.matches("^\\d{4}-\\d{2}-\\d{2}$")) dateUri = "http://reference.data.gov.uk/id/gregorian-day/"+attrVal;
                else if(attrVal.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$")) dateUri = "http://reference.data.gov.uk/id/gregorian-interval/"+attrVal;
                else if(attrVal.matches("^\\d{4}-\\d{2}$")) dateUri = "http://reference.data.gov.uk/id/gregorian-month/"+attrVal;
                else if(attrVal.matches("^\\d{4}$")) dateUri = "http://reference.data.gov.uk/id/gregorian-year/"+attrVal;
                else throw exceptionFactory.failure("Date value {} in column {}, dimension {} is not in xs:date/xs:dateTime format.", attrVal, attr.getColumn(), name);

                if(dateUri != null) {
                    output.submit(observation, this.valueProperty, Mapper.VALUE_FACTORY.createIRI(dateUri));
                    output.submit(Mapper.VALUE_FACTORY.createIRI(dateUri), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.TIME_INTERVAL));
                }
            }
        }
    }
}
