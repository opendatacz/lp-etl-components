package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.component.api.service.RdfToPojo;

/**
 *
 * @author Škoda Petr
 */
@RdfToPojo.Type(uri = FdpToRdfVocabulary.CONFIG)
public class FdpToRdfConfiguration {
    
    public FdpToRdfConfiguration() {
    }
    
    /*@RdfToPojo.Property(uri = FdpToRdfVocabulary.COLUMN_DEFS_QUERY)
    private String columnDefsQuery = "";
    
    public String getColumnDefsQuery() {
    	return columnDefsQuery;
    }*/

}
