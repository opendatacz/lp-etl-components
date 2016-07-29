package com.linkedpipes.plugin.loader.dcatAp11ToCkanBatch;

import com.linkedpipes.etl.component.api.service.RdfToPojo;

/**
 *
 * @author Klímek Jakub
 */
@RdfToPojo.Type(uri = DcatAp11ToCkanBatchConfigVocabulary.CONFIG_CLASS)
public class DcatAp11ToCkanBatchConfiguration {

    @RdfToPojo.Property(uri = DcatAp11ToCkanBatchConfigVocabulary.API_URL)
    private String apiUri;

    @RdfToPojo.Property(uri = DcatAp11ToCkanBatchConfigVocabulary.API_KEY)
    private String apiKey ;

    @RdfToPojo.Property(uri = DcatAp11ToCkanBatchConfigVocabulary.LOAD_LANGUAGE)
    private String loadLanguage;

    @RdfToPojo.Property(uri = DcatAp11ToCkanBatchConfigVocabulary.PROFILE)
    private String profile ;

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
}
