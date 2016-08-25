package com.linkedpipes.plugin.transformer.fdp.dimension;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;
import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.FdpHierarchicalAttribute;
import com.linkedpipes.plugin.transformer.fdp.FdpToRdfVocabulary;
import com.linkedpipes.plugin.transformer.fdp.Mapper;
import com.linkedpipes.plugin.transformer.fdp.dimension.FdpDimension;
import org.openrdf.model.IRI;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HierarchicalDimension extends FdpDimension {
    public static final String dimensionQuery =
            "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
                    "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\n" +
                    "\n" +
                    "SELECT DISTINCT ?dimensionProp ?dimensionName ?packageName ?dataset \n" +
                    "WHERE {\n" +
                    " ?component fdprdf:attributeCount ?attrCount .\n" +
                    "  FILTER(?attrCount > 1)\n" +
                    "  \n" +
                    "  ?dsd a qb:DataStructureDefinition;\n" +
                    "         qb:component ?component .\n" +
                    "  ?component qb:dimension ?dimensionProp;\n" +
                    "             fdprdf:attribute ?attribute ;\n" +
                    "             fdprdf:valueType fdprdf:skos .\n" +
                    "  ?attribute fdprdf:isHierarchical true .  \n" +
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
                    "             fdprdf:name ?attributeName ;\n" +
                    "             fdprdf:isHierarchical true.\n" +
                    "  \n" +
                            "OPTIONAL {?attribute fdprdf:parentAttribute [ fdprdf:name ?parentName ] .}" +
                    "  FILTER NOT EXISTS {?attribute fdprdf:labelfor [] .}\n" +
                    "}";

    public static final String labelQuery = "PREFIX qb: <http://purl.org/linked-data/cube#>\n" +
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
            "             fdprdf:name ?attributeName ;      \n" +
            "       \t\t fdprdf:labelfor ?labelForName ;\n" +
            "             fdprdf:source ?labelProperty .\n" +
            "}";

    private List<FdpHierarchicalAttribute> orderedAttributes;

    public HierarchicalDimension() {}

    public String getAttributeQueryTemplate() {
        return this.attributeQuery;
    }

    public String getLabelsQuery() {return insertDimensionIRI(labelQuery); }

    public void addLabel(String forAttribute, String labelColumn) {
        for(FdpAttribute attr : attributes) {
            FdpHierarchicalAttribute hAttr = (FdpHierarchicalAttribute) attr;
            if(hAttr.getName().equals(forAttribute)) hAttr.setLabel(labelColumn);
        }
    }

    private FdpHierarchicalAttribute getAttrByName(String name) {
        if(name == null) return null;
        for(FdpAttribute attr : attributes) {
            if(name.equals(((FdpHierarchicalAttribute) attr).getName())) return (FdpHierarchicalAttribute) attr;
        }
        return null;
    }

    @Override
    public void setAttributes(List<FdpAttribute> attributes) {
        this.attributes = attributes;
        FdpHierarchicalAttribute lowestAttr = null;
        for(FdpAttribute attr : attributes) {
            if (((FdpHierarchicalAttribute) attr).getParent() != null) {
                boolean isLowest = true;
                for (FdpAttribute anotherAttr : attributes) {
                    String parent = ((FdpHierarchicalAttribute) anotherAttr).getParent();
                    if (parent != null && parent.equals(((FdpHierarchicalAttribute) attr).getName())) isLowest = false;
                }
                if (isLowest) {
                    lowestAttr = (FdpHierarchicalAttribute) attr;
                    break;
                }
            }
        }
        orderedAttributes = new ArrayList<FdpHierarchicalAttribute>();
        while(lowestAttr!=null) {
            orderedAttributes.add(lowestAttr);
            lowestAttr = getAttrByName(lowestAttr.getParent());
        }
    }

    private IRI createAttrValIRI(FdpHierarchicalAttribute attr, String val) { return Mapper.VALUE_FACTORY.createIRI(datasetIri+"/"+attr.getName()+"/"+ urlEncode(val));}

    public void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException {
        boolean dimensionValFound = false;
        int i=0;
        for(FdpHierarchicalAttribute attr : orderedAttributes) {
            String attrVal = row.get(attr.getColumn());
            String attrLabel = row.get(attr.getLabelColumn());
            if (attrVal != null) {
                IRI attrValIRI = createAttrValIRI(attr, attrVal);
                if (!dimensionValFound) {
                    output.submit(observation, this.valueProperty, attrValIRI);
                    dimensionValFound = true;
                }
                if(i<orderedAttributes.size()-1) {
                    output.submit(attrValIRI,
                            Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_BROADER),
                            createAttrValIRI(orderedAttributes.get(i+1), row.get(orderedAttributes.get(i+1).getColumn())));
                }

                output.submit(attrValIRI, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_NOTATION), Mapper.VALUE_FACTORY.createLiteral(attrVal));
                output.submit(attrValIRI, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_CONCEPT));
                output.submit(attrValIRI, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_INSCHEME), getCodelistIRI());

                output.submit(getCodelistIRI(), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_CONCEPTSCHEME));

                if (orderedAttributes.indexOf(attr) == orderedAttributes.size() - 1)
                    output.submit(getCodelistIRI(), Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_HASTOPCONCEPT), attrValIRI);

                if (attrLabel != null)
                    output.submit(attrValIRI, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.SKOS_PREFLABEL), Mapper.VALUE_FACTORY.createLiteral(attrLabel));
            }
            i++;
        }

    }

}