package com.linkedpipes.plugin.transformer.fdp.dimension;

import java.io.IOException;
import java.util.*;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.Mapper;
import com.linkedpipes.plugin.transformer.fdp.dimension.FdpDimension;
import org.openrdf.model.*;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;

public class SingleAttributeLiteralDimension extends FdpDimension {
    public static final String attributeQuery =
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" +
                    "PREFIX qb: <http://purl.org/linked-data/cube#>\r\n" +
                    "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\r\n" +
                    "\r\n" +
                    "\r\n" +
                    "SELECT *" +
                    "WHERE {\r\n" +
                    "\r\n" +
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
            "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\n" +
            "\n" +
            "SELECT *\n" +
            "WHERE { \n" +
            "\n" +
            "  ?component fdprdf:attributeCount ?attrCount .\n" +
            "  FILTER(?attrCount = 1)\n" +
            "\t\n" +
            "  VALUES ( ?componentProperty \t\t?valueType) {\n" +
            "    ( \t   \tqb:dimension\t\t\tfdprdf:unknown)\n" +
            "    (\t\tqb:attribute\t\t\tfdprdf:unknown)\n" +
            "    (\t\tqb:componentProperty\tfdprdf:fact)\n" +
            "  } \n" +
            "\n" +
            "   ?dsd a qb:DataStructureDefinition ;\n" +
            "        qb:component ?component .\n" +
            "  ?component ?componentProperty ?dimensionProp ;\n" +
            "             fdprdf:attribute ?attribute ;\n" +
            "             fdprdf:valueType ?valueType .\n" +
            "             \n" +
            "   ?dimensionProp fdprdf:name ?dimensionName .  \n" +
            "                        \n" +
            "   ?dataset a qb:DataSet;\n" +
            "      fdprdf:datasetShortName ?packageName ;\n" +
            "      qb:structure ?dsd .\n" +
            "}  ";

    public SingleAttributeLiteralDimension(){}

    public void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException, IOException {
        //if(attributes.size()>1) throw exceptionFactory.failed("Single attribute dimension {} has more than one attribute.", name);
        for(FdpAttribute attr : attributes) {
            String attrVal = row.get(attr.getColumn());
            if(attrVal != null) {
                output.submit(observation, valueProperty, Mapper.VALUE_FACTORY.createLiteral(attrVal));
            }
        }
    }
}