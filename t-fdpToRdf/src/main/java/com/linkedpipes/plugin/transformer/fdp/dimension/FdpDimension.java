package com.linkedpipes.plugin.transformer.fdp.dimension;

import com.linkedpipes.etl.executor.api.v1.LpException;
import com.linkedpipes.etl.executor.api.v1.service.ExceptionFactory;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;

import com.linkedpipes.plugin.transformer.fdp.FdpAttribute;
import com.linkedpipes.plugin.transformer.fdp.Mapper;
import com.linkedpipes.plugin.transformer.fdp.PlainTextTripleWriter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;

public abstract class FdpDimension {
	protected IRI valueProperty;
	protected PlainTextTripleWriter output;
	protected List<FdpAttribute> attributes;
	protected String name;
	public abstract void processRow(IRI observation, HashMap<String, String> row, ExceptionFactory exceptionFactory) throws LpException, IOException;
	protected String datasetIri;
	protected String datasetName;
	protected IRI valueType = null;

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


	public String getLabelsQuery() {return insertDimensionIRI(labelQuery); }


	public void addLabel(String forAttribute, String labelColumn) {
		FdpAttribute labelAttribute = null;
		for(FdpAttribute attr : attributes) {
			if(attr.getName().equals(forAttribute)) attr.setLabel(labelColumn);
			if(attr.getColumn().equals(labelColumn)) labelAttribute = attr;
		}
		attributes.remove(labelAttribute);
	}

	public String getDimensionValIriBase() {
		return datasetIri+"/"+name+"/";
	}

	public void setAttributes(List<FdpAttribute> attributes) {
		this.attributes = attributes;
	}

	public void init(PlainTextTripleWriter consumer, IRI valueProperty, String name, String datasetIri, String datasetName) {
		this.output = consumer;
		this.valueProperty = valueProperty;
		this.name = name;
		this.datasetIri = datasetIri;
		this.datasetName = datasetName;
	}

	public void setValueType(IRI type) {valueType = type;}

	public abstract String getAttributeQueryTemplate();

	protected String insertDimensionIRI(String template) {
		return template.replace("_dimensionProp_", "<"+valueProperty.stringValue()+">");
	}

	public String getAttributeQuery() {
		return insertDimensionIRI(getAttributeQueryTemplate());
	}

	public Resource createValueIri(HashMap<String,String> row) {
		String valIri = getDimensionValIriBase()+mergedPrimaryKey(row);
		return Mapper.VALUE_FACTORY.createIRI(valIri);
	}

	public IRI getCodelistIRI() { return Mapper.VALUE_FACTORY.createIRI("http://data.openbudgets.eu/resource/"+datasetName+"/codelist/"+name);}

	public String mergedPrimaryKey(HashMap<String,String> row) {
		String key = "";
		boolean isFirst = true;
		for(FdpAttribute attr : attributes) {
			if(attr.isKey()) {
				if(!isFirst) {
					key += "-";
				}
				isFirst = false;
				key += urlEncode(row.get(attr.getColumn()));
			}
		}
		return key;
	}

	public static String urlEncode(String val)
	{
		try {
			return URLEncoder.encode(val, "UTF-8");
		}
		catch(Exception e) {
			return null;
		}
	}
}