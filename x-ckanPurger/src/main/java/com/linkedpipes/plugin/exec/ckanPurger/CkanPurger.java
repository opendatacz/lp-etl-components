package com.linkedpipes.plugin.exec.ckanPurger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.linkedpipes.etl.component.api.service.ProgressReport;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResults;
import org.openrdf.query.TupleQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedpipes.etl.dataunit.sesame.api.rdf.SingleGraphDataUnit;
import org.openrdf.query.TupleQueryResult;
import com.linkedpipes.etl.component.api.Component;
import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;
import org.openrdf.query.impl.SimpleDataset;

/**
 *
 * @author Klímek Jakub
 */
public final class CkanPurger implements Component.Sequential {

    private static final Logger LOG = LoggerFactory.getLogger(CkanPurger.class);

    @Component.Configuration
    public CkanPurgerConfiguration configuration;

    @Component.Inject
    public ExceptionFactory exceptionFactory;

    @Component.Inject
    public ProgressReport progressReport;

    private CloseableHttpClient queryClient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
    private CloseableHttpClient postClient = HttpClients.createDefault();

    private String apiURI;

    private List<String> getDatasets() {
        CloseableHttpResponse queryResponse = null;
        List<String> datasetList = new LinkedList<>();
        HttpGet httpGetOrg = new HttpGet(apiURI + "/package_list");

        LOG.debug("Querying CKAN for datasets");

        try {
            queryResponse = queryClient.execute(httpGetOrg);
            if (queryResponse.getStatusLine().getStatusCode() == 200) {
                JSONArray response = new JSONObject(EntityUtils.toString(queryResponse.getEntity())).getJSONArray("result");
                for (Object o : response) {
                    datasetList.add(o.toString());
                }
                LOG.info("Dataset list downloaded, found " + datasetList.size() + " datasets.");

            } else {
                String ent = EntityUtils.toString(queryResponse.getEntity());
                LOG.info("Dataset list not downloaded: " + ent);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        } finally {
            if (queryResponse != null) {
                try {
                    queryResponse.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            }
        }

        return datasetList;
    }

    private List<String> getOrganizations() {
        CloseableHttpResponse queryResponse = null;
        List<String> organizationList = new LinkedList<>();
        HttpGet httpGetOrg = new HttpGet(apiURI + "/organization_list");

        LOG.debug("Querying CKAN for organizations");

        try {
            queryResponse = queryClient.execute(httpGetOrg);
            if (queryResponse.getStatusLine().getStatusCode() == 200) {
                JSONArray response = new JSONObject(EntityUtils.toString(queryResponse.getEntity())).getJSONArray("result");
                for (Object o : response) {
                    organizationList.add(o.toString());
                }
                LOG.info("Organization list downloaded, found " + organizationList.size() + " organizations.");

            } else {
                String ent = EntityUtils.toString(queryResponse.getEntity());
                LOG.info("Organizations not downloaded: " + ent);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        } finally {
            if (queryResponse != null) {
                try {
                    queryResponse.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            }
        }

        return organizationList;
    }

    private void purgeDataset(String datasetID) throws LpException {
        JSONObject root = new JSONObject();
        root.put("id", datasetID);
        HttpPost httpPost = new HttpPost(apiURI + "/dataset_purge");
        httpPost.addHeader(new BasicHeader("Authorization", configuration.getApiKey()));
        String json = root.toString();
        httpPost.setEntity(new StringEntity(json, Charset.forName("utf-8")));
        CloseableHttpResponse response = null;

        try {
            response = postClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.debug("Dataset purged OK");
            } else {
                String ent = EntityUtils.toString(response.getEntity());
                LOG.error("Response:" + ent);
                throw exceptionFactory.failed("Error purging dataset: " + ent);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                    throw exceptionFactory.failed("Error purging dataset");
                }
            }
        }
    }

    private void purgeOrganization(String organizationID) throws LpException {
        JSONObject root = new JSONObject();
        root.put("id", organizationID);
        HttpPost httpPost = new HttpPost(apiURI + "/organization_purge");
        httpPost.addHeader(new BasicHeader("Authorization", configuration.getApiKey()));
        String json = root.toString();
        httpPost.setEntity(new StringEntity(json, Charset.forName("utf-8")));
        CloseableHttpResponse response = null;

        try {
            response = postClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.debug("Organization purged OK");
            } else {
                String ent = EntityUtils.toString(response.getEntity());
                LOG.error("Response:" + ent);
                throw exceptionFactory.failed("Error purging organization: " + ent);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                    throw exceptionFactory.failed("Error purging organization");
                }
            }
        }
    }

    @Override
    public void execute() throws LpException {

        apiURI = configuration.getApiUri();

        if (apiURI == null || apiURI.isEmpty() || configuration.getApiKey() == null || configuration.getApiKey().isEmpty() ) {
            throw exceptionFactory.failed("Missing required settings.");
        }

        List<String> organizations = getOrganizations();
        List<String> datasets = getDatasets();

        progressReport.start(datasets.size() + organizations.size());

        int current = 0;
        for (String datasetID : datasets) {
            current++;
            LOG.info("Purging dataset " + current + "/" + datasets.size() + ": " + datasetID);
            purgeDataset(datasetID);
            progressReport.entryProcessed();
        }

        current = 0;
        for (String organizaitonID : organizations) {
            current++;
            LOG.info("Purging organization " + current + "/" + organizations.size() + ": " + organizaitonID);
            purgeOrganization(organizaitonID);
            progressReport.entryProcessed();
        }

        try {
            queryClient.close();
            postClient.close();
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }

        progressReport.done();
    }
}
