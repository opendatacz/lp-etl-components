package com.linkedpipes.plugin.exec.ckanPurger;

import com.linkedpipes.etl.executor.api.v1.rdf.RdfToPojo;

@RdfToPojo.Type(iri = CkanPurgerConfigVocabulary.CONFIG_CLASS)
public class CkanPurgerConfiguration {

    @RdfToPojo.Property(iri = CkanPurgerConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(iri = CkanPurgerConfigVocabulary.API_KEY)
    private String apiKey;

    public CkanPurgerConfiguration() {
    }

    public String getApiUri() {
        return apiUri;
    }

    public void setApiUri(String apiUri) {
        this.apiUri = apiUri;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

}
