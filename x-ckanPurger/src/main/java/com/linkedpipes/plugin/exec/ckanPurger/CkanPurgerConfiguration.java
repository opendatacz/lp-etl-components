package com.linkedpipes.plugin.exec.ckanPurger;

import com.linkedpipes.etl.component.api.service.RdfToPojo;

/**
 *
 * @author Klímek Jakub
 */
@RdfToPojo.Type(uri = CkanPurgerConfigVocabulary.CONFIG_CLASS)
public class CkanPurgerConfiguration {

    @RdfToPojo.Property(uri = CkanPurgerConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(uri = CkanPurgerConfigVocabulary.API_KEY)
    private String apiKey ;

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
