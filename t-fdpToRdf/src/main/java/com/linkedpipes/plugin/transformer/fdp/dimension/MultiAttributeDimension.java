package com.linkedpipes.plugin.transformer.fdp.dimension;

import java.util.*;

import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.FdpToRdfVocabulary;
import com.linkedpipes.plugin.transformer.fdp.Mapper;
import com.linkedpipes.plugin.transformer.fdp.dimension.FdpDimension;
import org.openrdf.model.Resource;
import org.openrdf.model.*;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;

public class MultiAttributeDimension extends FdpDimension {
	public static final String attributeQuery = 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" + 
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n" + 
			"PREFIX fdp: <http://schemas.frictionlessdata.io/fiscal-data-package#>\r\n" + 
			"PREFIX fcsv: <file://budget.csv#>\r\n" + 
			"PREFIX obeu-attribute:   <http://data.openbudgets.eu/ontology/dsd/attribute/> \r\n" + 
			"PREFIX obeu-dimension:   <http://data.openbudgets.eu/ontology/dsd/dimension/> \r\n" + 
			"PREFIX obeu-measure:     <http://data.openbudgets.eu/ontology/dsd/measure/> \r\n" + 
			"PREFIX qb: <http://purl.org/linked-data/cube#>\r\n" + 
			"PREFIX datasets: <http://data.openbudgets.eu/datasets/>\r\n" + 
			"PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#>\r\n" + 
			"PREFIX schema: <http://schema.org/>\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"SELECT *" + 
			"WHERE {\r\n" + 
			"\r\n" + 
			" ?component fdprdf:attributeCount ?attrCount .\r\n" + 
			"  FILTER(?attrCount > 1)\r\n" + 
			"  \r\n" + 
			"  VALUES ( 	?valueType 			?rdfType 			?componentProperty ) {\r\n" + 
			"    ( 	   	fdprdf:organization	schema:Organization	qb:dimension	)\r\n" + 
			"    (		fdprdf:location		schema:Location		qb:attribute	)\r\n" + 
			"    (		fdprdf:unknown		UNDEF				qb:dimension	)\r\n" + 
			"    (		fdprdf:fact			UNDEF				qb:componentProperty	)\r\n" + 
			"  } \r\n" + 
			"  \r\n" + 
			"  ?dsd a qb:DataStructureDefinition;\r\n" + 
			"         qb:component ?component .\r\n" + 
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
					"PREFIX schema: <http://schema.org/>\n" +
					"\n" +
					"SELECT DISTINCT ?dimensionProp ?dimensionName ?packageName ?dataset WHERE {\n" +
					"\n" +
					" ?component fdprdf:attributeCount ?attrCount .\n" +
					"  FILTER(?attrCount > 1)\n" +
					"  \n" +
					"  VALUES ( \t?valueType \t\t\t?rdfType \t\t\t?componentProperty ) {\n" +
					"    ( \t   \tfdprdf:organization\tschema:Organization\tqb:dimension\t)\n" +
					"    (\t\tfdprdf:location\t\tschema:Location\t\tqb:attribute\t)\n" +
					"    (\t\tfdprdf:unknown\t\tUNDEF\t\t\t\tqb:dimension\t)\n" +
					"    (\t\tfdprdf:fact\t\t\tUNDEF\t\t\t\tqb:componentProperty\t)\n" +
					"  } \n" +
					"  \n" +
					"   ?dataset a qb:DataSet;\n" +
					"      fdprdf:datasetShortName ?packageName ;\n" +
					"      qb:structure ?dsd .  ?dsd a qb:DataStructureDefinition;\n" +
					"         qb:component ?component .\n" +
					"  ?component ?componentProperty ?dimensionProp ;\n" +
					"    fdprdf:valueType ?valueType .\n" +
					"  \n" +
					"  ?dimensionProp fdprdf:name ?dimensionName .\n" +
					"  \n" +
					"  {\n" +
					"    SELECT ?component (COUNT(DISTINCT ?attribute) AS ?nonHierarchCount)\n" +
					"    WHERE {\n" +
					"        ?component fdprdf:attribute ?attribute .\n" +
					"      FILTER NOT EXISTS {?attribute fdprdf:isHierarchical true .}\n" +
					"    } GROUPBY ?component\n" +
					"  }   \n" +
					"  FILTER (?attrCount = ?nonHierarchCount)\n" +
					"}";
	
	public MultiAttributeDimension(){}
	
	public void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException {
		Resource dimensionVal = createValueIri(row);
		if(valueType!=null) output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A), valueType);
		for(FdpAttribute attr : attributes) {
			String attrVal = row.get(attr.getColumn());
			if(attrVal != null) {
				output.submit(dimensionVal, Mapper.VALUE_FACTORY.createIRI(attr.getPartialPropertyIri().stringValue()), Mapper.VALUE_FACTORY.createLiteral(attrVal));
			}
		}
		output.submit(observation, this.valueProperty, dimensionVal);
	}
}