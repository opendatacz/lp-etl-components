package com.linkedpipes.plugin.transformer.fdp;

import org.openrdf.model.Resource;

/**
 * Created by admin on 22.8.2016.
 */
public class FdpHierarchicalAttribute extends FdpAttribute {
    private String name;
    private String parentName = null;
    private String labelColumn = null;

    public FdpHierarchicalAttribute(String sourceColumn, String sourceFile, boolean isKey, Resource propertyIri, String name) {
        super(sourceColumn, sourceFile, isKey, propertyIri);
        this.name = name;
    }

    public void setParent(String parentName) {
        this.parentName = parentName;
    }

    public void setLabel(String labelColumn) {
        this.labelColumn = labelColumn;
    }

    public String getName() {return this.name;}

    public String getParent() { return parentName; }

    public String getLabelColumn() { return labelColumn; }
}
