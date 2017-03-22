package com.linkedpipes.plugin.exec.ckanPurger;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;

/**
 * Represent a CKAN instance.
 * @author Petr Škoda
 */
class Ckan {

    private final String apiUrl;

    private final String apiKey;

    Ckan(String ckanUrl, String apiKey) {
        this.apiUrl = ckanUrl;
        this.apiKey = apiKey;
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void addAuthorizationHeader(HttpPost request) {
        request.addHeader(new BasicHeader("Authorization", apiKey));
    }

}
