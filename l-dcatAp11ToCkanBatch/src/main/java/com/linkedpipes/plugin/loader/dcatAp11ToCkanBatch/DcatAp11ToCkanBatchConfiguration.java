package com.linkedpipes.plugin.loader.dcatAp11ToCkanBatch;

import com.linkedpipes.etl.executor.api.v1.rdf.RdfToPojo;

@RdfToPojo.Type(iri = DcatAp11ToCkanBatchConfigVocabulary.CONFIG_CLASS)
public class DcatAp11ToCkanBatchConfiguration {

    @RdfToPojo.Property(iri = DcatAp11ToCkanBatchConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(iri = DcatAp11ToCkanBatchConfigVocabulary.API_KEY)
    private String apiKey;

    @RdfToPojo.Property(iri = DcatAp11ToCkanBatchConfigVocabulary.LOAD_LANGUAGE)
    private String loadLanguage;

    @RdfToPojo.Property(iri = DcatAp11ToCkanBatchConfigVocabulary.PROFILE)
    private String profile;

    @RdfToPojo.Property(iri = DcatAp11ToCkanBatchConfigVocabulary.FILE_OUTPUT)
    private Boolean toFile;

    @RdfToPojo.Property(iri = DcatAp11ToCkanBatchConfigVocabulary.API_OUTPUT)
    private Boolean toApi;

    public DcatAp11ToCkanBatchConfiguration() {
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

    public String getLoadLanguage() {
        return loadLanguage;
    }

    public void setLoadLanguage(String loadLanguage) {
        this.loadLanguage = loadLanguage;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public Boolean getToFile() {
        return toFile;
    }

    public void setToFile(Boolean toFile) {
        this.toFile = toFile;
    }

    public Boolean getToApi() {
        return toApi;
    }

    public void setToApi(Boolean toApi) {
        this.toApi = toApi;
    }
}
