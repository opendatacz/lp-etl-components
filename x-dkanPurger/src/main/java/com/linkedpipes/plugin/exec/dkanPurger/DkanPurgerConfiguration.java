package com.linkedpipes.plugin.exec.dkanPurger;

import com.linkedpipes.etl.executor.api.v1.rdf.RdfToPojo;


@RdfToPojo.Type(iri = DkanPurgerConfigVocabulary.CONFIG_CLASS)
public class DkanPurgerConfiguration {

    @RdfToPojo.Property(iri = DkanPurgerConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(iri = DkanPurgerConfigVocabulary.USERNAME)
    private String username;

    @RdfToPojo.Property(iri = DkanPurgerConfigVocabulary.PASSWORD)
    private String password;

    public DkanPurgerConfiguration() {
    }

    public String getApiUri() {
        return apiUri;
    }

    public void setApiUri(String apiUri) {
        this.apiUri = apiUri;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
