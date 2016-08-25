package com.linkedpipes.plugin.transformer.fdp;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.model.vocabulary.RDF;
import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;

public class FdpAttribute {
	//private String name;
	private String sourceColumn;
	private String sourceFile;
	private boolean bIsKey;
	private Resource partialPropertyIri;
	public Resource getPartialPropertyIri(){
		return partialPropertyIri; // Mapper.VALUE_FACTORY.createIri(partialPropertyIri);
	}
	public boolean isKey() {return bIsKey;}
	public String getColumn() {return sourceColumn;} 
	public FdpAttribute(String sourceColumn, String sourceFile, boolean isKey, Resource propertyIri) {
		//this.name = name;
		this.sourceColumn = sourceColumn;
		this.sourceFile = sourceFile;
		this.bIsKey = isKey;
		this.partialPropertyIri = propertyIri;
	}
}