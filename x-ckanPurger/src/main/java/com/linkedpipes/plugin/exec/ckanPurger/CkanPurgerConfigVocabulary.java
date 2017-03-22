package com.linkedpipes.plugin.exec.ckanPurger;

public class CkanPurgerConfigVocabulary {

    private static final String PREFIX =
            "http://plugins.linkedpipes.com/ontology/x-ckanPurger#";

    public static final String CONFIG_CLASS = PREFIX + "Configuration";

    public static final String API_URL = PREFIX + "apiUrl";

    public static final String API_KEY = PREFIX + "apiKey";

    public static final String PURGE_ALL_DATASETS =
            PREFIX + "purgeAllDatasets";

    public static final String PURGE_ALL_ORGANIZATIONS =
            PREFIX + "purgeAllOrganizations";

    public static final String FAIL_ON_ERROR = PREFIX + "failOnError";
    
}
