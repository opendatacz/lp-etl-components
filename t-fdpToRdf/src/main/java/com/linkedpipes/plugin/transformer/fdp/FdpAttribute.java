package com.linkedpipes.plugin.transformer.fdp;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import org.eclipse.rdf4j.model.Resource;

public class FdpAttribute {
	//private String name;
    public enum Format {NUMBER, DATE, UNDEFINED}
    private Format format = Format.UNDEFINED;
	private String sourceColumn;
	private String sourceFile;
    private char decimalSep = '.';
    private char groupSep = ' ';
	private boolean bIsKey;
	private Resource partialPropertyIri;
    private String labelColumn = null;
    private String name;


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
	public void setFormat(String format) {
		if(format.equals("number")) this.format = Format.NUMBER;
        else this.format = Format.UNDEFINED;
	}

	public void setFormat(Format valueFormat) {
	    this.format = valueFormat;
    }

	public void setDecimalSep(char decimalSep) {
	    this.decimalSep = decimalSep;
    }
    public void setGroupSep(char groupSep) {
        this.groupSep = groupSep;
    }
	public Object parseValue(String value) {
        Object result = null;
	    switch(this.format) {
            case NUMBER: {
                DecimalFormat df = new DecimalFormat();
                DecimalFormatSymbols symbols = new DecimalFormatSymbols();
                symbols.setDecimalSeparator(decimalSep);
                symbols.setGroupingSeparator(groupSep);
                df.setDecimalFormatSymbols(symbols);
                try {
                    result = df.parse(value).doubleValue();
                } catch (Exception e) {
                    result = null;
                }
            } break;
        }
        return result;
    }

    public String getLabelColumn() { return labelColumn; }
    public void setLabel(String labelColumn) {
        this.labelColumn = labelColumn;
    }

    public String getName() {return this.name;}
    public void setName(String name) {this.name = name;}

}