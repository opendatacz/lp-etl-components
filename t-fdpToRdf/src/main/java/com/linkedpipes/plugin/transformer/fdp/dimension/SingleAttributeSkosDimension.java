package com.linkedpipes.plugin.transformer.fdp.dimension;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;
import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.FdpToRdfVocabulary;
import com.linkedpipes.plugin.transformer.fdp.Mapper;
import org.openrdf.model.IRI;
import org.openrdf.model.Resource;

import java.util.HashMap;

public class SingleAttributeSkosDimension extends FdpDimension {
    public static final String dimensionQuery =
            "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
                    "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\n" +
                    "\n" +
                    "SELECT DISTINCT ?dimensionProp ?dimensionName ?packageName ?dataset \n" +
                    "WHERE {\n" +
                    " ?component fdprdf:attributeCount ?attrCount .\n" +
                    "  FILTER(?attrCount = 1)\n" +
                    "  \n" +
                    "  ?dsd a qb:DataStructureDefinition;\n" +
                    "         qb:component ?component .\n" +
                    "  ?component qb:dimension ?dimensionProp;\n" +
                    "             fdprdf:attribute ?attribute ;\n" +
                    "             fdprdf:valueType fdprdf:skos .\n" +
                    "             \n" +
                    "  ?dimensionProp fdprdf:name ?dimensionName .\n" +
                    "  \n" +
                    "  ?dataset a qb:DataSet;  \n" +
                    "      fdprdf:datasetShortName ?packageName ;\n" +
                    "      qb:structure ?dsd .    \n" +
                    "}";

    public static final String attributeQuery =
            "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
                    "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\n" +
                    "\n" +
                    "SELECT *\n" +
                    "WHERE {\n" +
                    "  ?component qb:dimension _dimensionProp_;\n" +
                    "             fdprdf:attribute ?attribute ;\n" +
                    "             fdprdf:valueType fdprdf:skos .             \n" +
                    "  \n" +
                    "  ?attribute fdprdf:sourceColumn ?sourceColumn ;\n" +
                    "\t\t\t fdprdf:sourceFile ?sourceFile;\n" +
                    "\t\t\t fdprdf:iskey ?iskey;\n" +
                    "             fdprdf:valueProperty ?attributeValueProperty;\n" +
                    "}";

    public SingleAttributeSkosDimension() {}

    public String getAttributeQueryTemplate() {
        return this.attributeQuery;
    }

    public void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException {
        Resource dimensionVal = createValueIri(row);
            String attrVal = row.get(attributes.get(0).getColumn());
            if(attrVal != null) {
                output.submit(observation, this.valueProperty, dimensionVal);
                output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_CONCEPT));
                output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_PREFLABEL), Mapper.VALUE_FACTORY.createLiteral(attrVal));
                output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_INSCHEME), getCodelistIRI());
                output.submit(getCodelistIRI(), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_CONCEPTSCHEME));
                output.submit(getCodelistIRI(), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_HASTOPCONCEPT), dimensionVal);
            }

    }

}