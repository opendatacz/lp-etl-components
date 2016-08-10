package com.linkedpipes.plugin.exec.dkanPurger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.linkedpipes.etl.component.api.service.ProgressReport;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedpipes.etl.component.api.Component;
import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;

/**
 *
 * @author Klímek Jakub
 */
public final class DkanPurger implements Component.Sequential {

    private static final Logger LOG = LoggerFactory.getLogger(DkanPurger.class);

    @Component.Configuration
    public DkanPurgerConfiguration configuration;

    @Component.Inject
    public ExceptionFactory exceptionFactory;

    @Component.Inject
    public ProgressReport progressReport;

    private CloseableHttpClient queryClient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
    private CloseableHttpClient postClient = HttpClients.createDefault();

    private String apiURI;

    private String token;

    private String getToken(String username, String password) throws LpException {
        HttpPost httpPost = new HttpPost(apiURI + "/user/login");

        ArrayList<NameValuePair> postParameters = new ArrayList<>();
        postParameters.add(new BasicNameValuePair("username", username));
        postParameters.add(new BasicNameValuePair("password", password));

        httpPost.addHeader(new BasicHeader("Accept", "application/json"));

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(postParameters));
        } catch (UnsupportedEncodingException e) {
            LOG.error("Unexpected encoding issue");
        }

        CloseableHttpResponse response = null;
        String token = null;
        try {
            response = postClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.debug("Logged in");
                token = new JSONObject(EntityUtils.toString(response.getEntity())).getString("token");
            } else {
                String ent = EntityUtils.toString(response.getEntity());
                LOG.error("Response:" + ent);
                throw exceptionFactory.failed("Error logging in: " + ent);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                    throw exceptionFactory.failed("Error logging in");
                }
            }
        }

        return token;
    }

    private List<String> getNodes() {
        CloseableHttpResponse queryResponse = null;
        List<String> nodeList = new LinkedList<>();
        HttpGet httpGetOrg = new HttpGet(apiURI + "/node");

        LOG.debug("Querying DKAN for nodes");

        try {
            queryResponse = queryClient.execute(httpGetOrg);
            if (queryResponse.getStatusLine().getStatusCode() == 200) {
                JSONArray response = new JSONArray(EntityUtils.toString(queryResponse.getEntity()));
                for (Object o : response) {
                    nodeList.add(new JSONObject(o.toString()).getString("uri"));
                }
                LOG.info("Node list downloaded, found " + nodeList.size() + " nodes.");

            } else {
                String ent = EntityUtils.toString(queryResponse.getEntity());
                LOG.info("Node list not downloaded: " + ent);
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

        return nodeList;
    }

    private void purgeNode(String nodeID) throws LpException {
        HttpDelete httpDelete = new HttpDelete(nodeID);
        httpDelete.addHeader(new BasicHeader("X-CSRF-Token", token));
        CloseableHttpResponse response = null;

        try {
            response = postClient.execute(httpDelete);
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.debug("Node deleted OK");
            } else {
                String ent = EntityUtils.toString(response.getEntity());
                LOG.error("Response:" + ent);
                throw exceptionFactory.failed("Error purging node: " + ent);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                    throw exceptionFactory.failed("Error purging node");
                }
            }
        }
    }

      @Override
    public void execute() throws LpException {

        apiURI = configuration.getApiUri();

        if (apiURI == null
                || apiURI.isEmpty()
                || configuration.getUsername() == null
                || configuration.getUsername().isEmpty()
                || configuration.getPassword() == null
                || configuration.getPassword().isEmpty()
                ) {
            throw exceptionFactory.failed("Missing required settings.");
        }

        token = getToken(configuration.getUsername(), configuration.getPassword());

        List<String> nodes = getNodes();

        progressReport.start(nodes.size());

        int current = 0;
        for (String nodeID : nodes) {
            current++;
            LOG.info("Purging node " + current + "/" + nodes.size() + ": " + nodeID);
            purgeNode(nodeID);
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
