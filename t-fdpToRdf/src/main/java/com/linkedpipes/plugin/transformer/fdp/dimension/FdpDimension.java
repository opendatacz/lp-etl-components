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