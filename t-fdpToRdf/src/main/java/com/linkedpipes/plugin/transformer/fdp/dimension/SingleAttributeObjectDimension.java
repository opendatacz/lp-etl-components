package com.linkedpipes.plugin.transformer.fdp.dimension;

import java.io.IOException;
import java.util.*;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.FdpToRdfVocabulary;
import com.linkedpipes.plugin.transformer.fdp.Mapper;
import com.linkedpipes.plugin.transformer.fdp.dimension.FdpDimension;
import org.openrdf.model.Resource;
import org.openrdf.model.*;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;

public class SingleAttributeObjectDimension extends FdpDimension {
    public static final String attributeQuery =
            "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>" +
                    "PREFIX schema: <http://schema.org/>\n" +
                    "PREFIX qb: <http://purl.org/linked-data/cube#>\r\n" +
                    "\n" +
                    "SELECT *\n" +
                    "WHERE { \n" +
                    "VALUES ( 	?valueType 			?rdfType 			?componentProperty 		?attrValueProperty ) {" +
        "    ( \t   \tfdprdf:organization\tschema:Organization\tqb:dimension\t\t\tschema:name )\n" +
                "    (\t\tfdprdf:location\t\tschema:Location\t\tqb:attribute\t\t\tschema:name )\n" +
                    "}" +
            "  ?component ?componentProperty _dimensionProp_;\r\n" +
                    "             fdprdf:attribute ?attribute ;\r\n" +
                    "             fdprdf:valueType ?valueType .\r\n" +
                    "             \r\n" +
                    "  \r\n" +
                    "  ?attribute fdprdf:sourceColumn ?sourceColumn ;\r\n"
                    + "			  fdprdf:sourceFile ?sourceFile;"
                    + "			  fdprdf:iskey ?iskey;" +
                    "             fdprdf:valueProperty ?attributeValueProperty .\r\n" +
                    "  FILTER NOT EXISTS {?attribute fdprdf:isHierarchical true .}\r\n" +
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
            "\n" +
            "  VALUES ( \t?valueType \t\t\t?rdfType \t\t\t?componentProperty \t\t?attrValueProperty ) {\n" +
            "    ( \t   \tfdprdf:organization\tschema:Organization\tqb:dimension\t\t\tschema:name )\n" +
            "    (\t\tfdprdf:location\t\tschema:Location\t\tqb:attribute\t\t\tschema:name )\n" +
            "  } \n" +
            "  \n" +
            "  ?dsd a qb:DataStructureDefinition ;\n" +
            "         qb:component ?component .\n" +
            "  ?component ?componentProperty ?dimensionProp;\n" +
            "             fdprdf:attribute ?attribute;\n" +
            "             fdprdf:valueType ?valueType .\n" +
            "                          \n" +
            "   ?dataset a qb:DataSet;" +
            "      fdprdf:datasetShortName ?packageName ;\n" +
            "      qb:structure ?dsd ." +
            "  ?dimensionProp fdprdf:name ?dimensionName .\r\n" +
                "}  ";

    public SingleAttributeObjectDimension(){}

    public void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException, IOException {
        Resource dimensionVal = createValueIri(row);
        if(valueType!=null) output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), valueType);
        for(FdpAttribute attr : attributes) {
            String attrVal = row.get(attr.getColumn());
            if(attrVal != null) {
                output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SCHEMA_NAME), Mapper.VALUE_FACTORY.createLiteral(attrVal));
            }
        }
        output.submit(observation, this.valueProperty, dimensionVal);
    }
}