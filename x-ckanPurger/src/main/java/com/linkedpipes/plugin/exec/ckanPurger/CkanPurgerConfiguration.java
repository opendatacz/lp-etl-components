package com.linkedpipes.plugin.exec.ckanPurger;

import com.linkedpipes.etl.executor.api.v1.rdf.RdfToPojo;

@RdfToPojo.Type(iri = CkanPurgerConfigVocabulary.CONFIG_CLASS)
public class CkanPurgerConfiguration {

    @RdfToPojo.Property(iri = CkanPurgerConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(iri = CkanPurgerConfigVocabulary.API_KEY)
    private String apiKey;

    @RdfToPojo.Property(iri = CkanPurgerConfigVocabulary.PURGE_ALL_DATASETS)
    private boolean purgeAllDatasets = false;

    @RdfToPojo.Property(
            iri = CkanPurgerConfigVocabulary.PURGE_ALL_ORGANIZATIONS)
    private boolean purgeAllOrganizations = false;

    @RdfToPojo.Property(iri = CkanPurgerConfigVocabulary.FAIL_ON_ERROR)
    private boolean failOnError = false;

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

    public boolean isPurgeAllDatasets() {
        return purgeAllDatasets;
    }

    public void setPurgeAllDatasets(boolean purgeAllDatasets) {
        this.purgeAllDatasets = purgeAllDatasets;
    }

    public boolean isPurgeAllOrganizations() {
        return purgeAllOrganizations;
    }

    public void setPurgeAllOrganizations(boolean purgeAllOrganizations) {
        this.purgeAllOrganizations = purgeAllOrganizations;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

}
