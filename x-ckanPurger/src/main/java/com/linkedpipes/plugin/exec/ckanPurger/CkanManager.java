package com.linkedpipes.plugin.exec.ckanPurger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.json.JSONObject;

/**
 * Perform operations over given CKAN instance.
 *
 * @author Petr Škoda
 */
class CkanManager {

    public static class CkanException extends Exception {

        public CkanException(String message) {
            super(message);
        }

        public CkanException(String message, Throwable cause) {
            super(message, cause);
        }

    }

    private final HttpRequestExecutor requestExecutor;

    private final Ckan ckan;

    CkanManager(HttpRequestExecutor requestExecutor, Ckan ckan) {
        this.requestExecutor = requestExecutor;
        this.ckan = ckan;
    }

    public List<String> getDatasets() throws CkanException {
        try {
            HttpGet httpGetOrg = new HttpGet(
                    ckan.getApiUrl() + "/package_list");
            return requestExecutor.executeHttpGetAndReturnResult(httpGetOrg);
        } catch (IOException | HttpRequestExecutor.RequestFailed ex) {
            throw new CkanException("Can't query all datasets.", ex);
        }
    }

    public void deleteDataset(String dataset) throws CkanException {
        HttpPost request = createDeleteDatasetRequest(dataset);
        try {
            requestExecutor.executeHttpPost(request);
        } catch (IOException | HttpRequestExecutor.RequestFailed ex) {
            throw new CkanException("Can't delete dataset: " + dataset, ex);
        }
    }

    private HttpPost createDeleteDatasetRequest(String dataset) {
        HttpPost httpPost = new HttpPost(ckan.getApiUrl() + "/dataset_purge");
        ckan.addAuthorizationHeader(httpPost);
        httpPost.setEntity(new StringEntity(
                createJsonWithId(dataset),
                Charset.forName("utf-8")));
        return httpPost;
    }

    private String createJsonWithId(String value) {
        JSONObject json = new JSONObject();
        json.put("id", value);
        return json.toString();
    }

    public List<String> getOrganizations() throws CkanException {
        try {
            HttpGet httpGetOrg = new HttpGet(
                    ckan.getApiUrl() + "/organization_list");
            return requestExecutor.executeHttpGetAndReturnResult(httpGetOrg);
        } catch (IOException | HttpRequestExecutor.RequestFailed ex) {
            throw new CkanException("Can't query all organizations.", ex);
        }
    }

    public void deleteOrganization(String organization) throws CkanException {
        HttpPost request = createDeleteOrganizationRequest(organization);
        try {
            requestExecutor.executeHttpPost(request);
        } catch (IOException | HttpRequestExecutor.RequestFailed ex) {
            throw new CkanException(
                    "Can't delete organization: " + organization, ex);
        }
    }

    private HttpPost createDeleteOrganizationRequest(String dataset) {
        HttpPost httpPost = new HttpPost(
                ckan.getApiUrl() + "/organization_purge");
        ckan.addAuthorizationHeader(httpPost);
        httpPost.setEntity(new StringEntity(
                createJsonWithId(dataset),
                Charset.forName("utf-8")));
        return httpPost;
    }

}
