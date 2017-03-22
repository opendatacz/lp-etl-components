package com.linkedpipes.plugin.exec.ckanPurger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute given HTTP operations.
 * Can read and parse response JSON objects.
 */
class HttpRequestExecutor {

    public class RequestFailed extends Exception {

        public RequestFailed(String message) {
            super(message);
        }

        public RequestFailed(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final Logger LOG
            = LoggerFactory.getLogger(HttpRequestExecutor.class);

    private final CloseableHttpClient queryClient;

    private final CloseableHttpClient postClient;

    HttpRequestExecutor() {
        queryClient = HttpClientBuilder.create().setRedirectStrategy(
                new LaxRedirectStrategy()).build();
        postClient = HttpClients.createDefault();
    }

    public List<String> executeHttpGetAndReturnResult(HttpGet httpGet)
            throws IOException, RequestFailed {
        try (CloseableHttpResponse response = queryClient.execute(httpGet)) {
            checkResponse(response);
            return readResponse(response);
        }
    }

    private void checkResponse(HttpResponse response)
            throws IOException, RequestFailed {
        if (response.getStatusLine().getStatusCode() != 200) {
            String error = EntityUtils.toString(response.getEntity());
            throw new RequestFailed("Request failed: " + error);
        }
    }

    private List<String> readResponse(CloseableHttpResponse response)
            throws IOException {
        String responseAsString = EntityUtils.toString(response.getEntity());
        JSONObject responseJson = new JSONObject(responseAsString);
        JSONArray resultArray = responseJson.getJSONArray("result");
        List<String> result = new ArrayList<>(resultArray.length());
        for (Object item : resultArray) {
            result.add(item.toString());
        }
        return result;
    }

    public void executeHttpPost(HttpPost httpPost)
            throws IOException, RequestFailed {
        try (CloseableHttpResponse response = postClient.execute(httpPost)) {
            checkResponse(response);
        }
    }

    public void close() {
        try {
            queryClient.close();
        } catch (Exception ex) {
            LOG.error("Can't close query client.", ex);
        }
        try {
            postClient.close();
        } catch (Exception ex) {
            LOG.error("Can't close post client.", ex);
        }
    }

}
