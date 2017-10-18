package com.linkedpipes.plugin.transformer.fdp;

import org.eclipse.rdf4j.model.Resource;


/**
 * Created by admin on 22.8.2016.
 */
public class FdpHierarchicalAttribute extends FdpAttribute {
    private String parentName = null;

    public FdpHierarchicalAttribute(String sourceColumn, String sourceFile, boolean isKey, Resource propertyIri, String name) {
        super(sourceColumn, sourceFile, isKey, propertyIri);
        this.setName(name);
    }

    public void setParent(String parentName) {
        this.parentName = parentName;
    }


    public String getParent() { return parentName; }
}
