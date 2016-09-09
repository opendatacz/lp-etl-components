package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.component.api.service.RdfToPojo;

@RdfToPojo.Type(uri = FdpToRdfVocabulary.CONFIG)
public class FdpToRdfConfiguration {

    public FdpToRdfConfiguration() {
    }

    @RdfToPojo.Property(uri = FdpToRdfVocabulary.HAS_FILE_NAME)
    private String fileName;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
