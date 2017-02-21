package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.executor.api.v1.rdf.RdfToPojo;


@RdfToPojo.Type(iri = FdpToRdfVocabulary.CONFIG)
public class FdpToRdfConfiguration {

    public FdpToRdfConfiguration() {
    }

    @RdfToPojo.Property(iri = FdpToRdfVocabulary.HAS_FILE_NAME)
    private String fileName;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
