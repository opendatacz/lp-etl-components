package com.linkedpipes.plugin.loader.dcatAp11ToDkanBatch;

import com.linkedpipes.etl.executor.api.v1.rdf.RdfToPojo;


@RdfToPojo.Type(iri = DcatAp11ToDkanBatchConfigVocabulary.CONFIG_CLASS)
public class DcatAp11ToDkanBatchConfiguration {

    @RdfToPojo.Property(iri = DcatAp11ToDkanBatchConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(iri = DcatAp11ToDkanBatchConfigVocabulary.USERNAME)
    private String username;

    @RdfToPojo.Property(iri = DcatAp11ToDkanBatchConfigVocabulary.PASSWORD)
    private String password;

    @RdfToPojo.Property(iri = DcatAp11ToDkanBatchConfigVocabulary.LOAD_LANGUAGE)
    private String loadLanguage;

    @RdfToPojo.Property(iri = DcatAp11ToDkanBatchConfigVocabulary.PROFILE)
    private String profile ;

    public DcatAp11ToDkanBatchConfiguration() {
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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
