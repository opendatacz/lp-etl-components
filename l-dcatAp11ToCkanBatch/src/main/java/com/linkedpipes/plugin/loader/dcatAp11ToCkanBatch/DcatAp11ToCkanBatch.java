package com.linkedpipes.plugin.loader.dcatAp11ToCkanBatch;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.linkedpipes.etl.component.api.service.ProgressReport;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
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
import org.json.JSONException;
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
public final class DcatAp11ToCkanBatch implements Component.Sequential {

    private static final Logger LOG = LoggerFactory.getLogger(DcatAp11ToCkanBatch.class);

    @Component.InputPort(id = "Metadata")
    public SingleGraphDataUnit metadata;

    @Component.Configuration
    public DcatAp11ToCkanBatchConfiguration configuration;

    @Component.Inject
    public ExceptionFactory exceptionFactory;

    @Component.Inject
    public ProgressReport progressReport;

    @Override
    public void execute() throws LpException {
        String apiURI = configuration.getApiUri();

        if (apiURI == null || apiURI.isEmpty() || configuration.getApiKey() == null || configuration.getApiKey().isEmpty() ) {
            throw exceptionFactory.failed("Missing required settings.");
        }

        LOG.debug("Querying CKAN for organizations");

        Map<String, String> organizations = new HashMap<>();
        List<String> organizationList = new LinkedList<>();

        CloseableHttpClient queryClient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
        CloseableHttpClient createClient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
        CloseableHttpClient postClient = HttpClients.createDefault();
        CloseableHttpResponse queryResponse = null;

        HttpGet httpGetOrg = new HttpGet(apiURI + "/organization_list");
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
        } catch (ClientProtocolException e) {
            LOG.error(e.getLocalizedMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        } catch (ParseException e) {
            LOG.error(e.getLocalizedMessage(), e);
        } catch (JSONException e) {
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

        LOG.debug("Querying for organization details.");

        for (String organization : organizationList) {
            HttpGet httpGetOrgDetail = new HttpGet(apiURI + "/organization_show?id=" + organization);
            try {
                queryResponse = queryClient.execute(httpGetOrgDetail);
                if (queryResponse.getStatusLine().getStatusCode() == 200) {
                    LOG.info("Organization " + organization + " downloaded");
                    JSONObject response = new JSONObject(EntityUtils.toString(queryResponse.getEntity())).getJSONObject("result");
                    JSONArray org_extras = response.getJSONArray("extras");
                    for (Object extra : org_extras) {
                        String extraKey = ((JSONObject) extra).getString("key");
                        String extraValue = ((JSONObject) extra).getString("value");
                        if (extraKey.equals("uri")) {
                            organizations.put(extraValue, organization);
                            break;
                        }
                    }
                } else {
                    String ent = EntityUtils.toString(queryResponse.getEntity());
                    LOG.info("Organization " + organization + " not downloaded: " + ent);
                }
            } catch (ClientProtocolException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (ParseException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (JSONException e) {
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
        }

        // Load files.
        LOG.debug("Querying metadata for datasets");

        LinkedList<String> datasets = new LinkedList<>();
        for (Map<String,Value> map: executeSelectQuery("SELECT ?d WHERE {?d a <" + DcatAp11ToCkanBatchVocabulary.DCAT_DATASET_CLASS + ">}")) {
            datasets.add(map.get("d").stringValue());
        }

        int current = 0;
        int total = datasets.size();

        JSONArray outArray = new JSONArray();

        LOG.info("Found " + total + " datasets");
        progressReport.start(total);

        for (String datasetURI : datasets) {
            current++;
            LOG.info("Processing dataset " + current + "/" + total + ": " + datasetURI);

            String datasetID = executeSimpleSelectQuery("SELECT ?did WHERE {<" + datasetURI + "> <"+ DcatAp11ToCkanBatchVocabulary.LODCZCKAN_DATASET_ID + "> ?did }", "did");
            if (datasetID.isEmpty()) {
                LOG.warn("Dataset " + datasetURI + " has missing CKAN ID");
                continue;
            }

            String title = executeSimpleSelectQuery("SELECT ?title WHERE {<" + datasetURI + "> <" + DCTERMS.TITLE + "> ?title FILTER(LANGMATCHES(LANG(?title), \"" + configuration.getLoadLanguage() + "\"))}", "title");
            String description = executeSimpleSelectQuery("SELECT ?description WHERE {<" + datasetURI + "> <" + DCTERMS.DESCRIPTION + "> ?description FILTER(LANGMATCHES(LANG(?description), \"" + configuration.getLoadLanguage() + "\"))}", "description");
            String curatorName = executeSimpleSelectQuery("SELECT ?name WHERE {<" + datasetURI + "> <" + DcatAp11ToCkanBatchVocabulary.DCAT_CONTACT_POINT + ">/<" + DcatAp11ToCkanBatchVocabulary.VCARD_FN + "> ?name }", "name");
            String contactPoint = executeSimpleSelectQuery("SELECT ?contact WHERE {<" + datasetURI + "> <" + DcatAp11ToCkanBatchVocabulary.DCAT_CONTACT_POINT + ">/<" + DcatAp11ToCkanBatchVocabulary.VCARD_HAS_EMAIL + "> ?contact }", "contact");
            String issued = executeSimpleSelectQuery("SELECT ?issued WHERE {<" + datasetURI + "> <" + DCTERMS.ISSUED + "> ?issued }", "issued");
            String modified = executeSimpleSelectQuery("SELECT ?modified WHERE {<" + datasetURI + "> <" + DCTERMS.MODIFIED + "> ?modified }", "modified");
            String publisher_uri = executeSimpleSelectQuery("SELECT ?publisher_uri WHERE {<" + datasetURI + "> <" + DCTERMS.PUBLISHER + "> ?publisher_uri }", "publisher_uri");
            String publisher_name = executeSimpleSelectQuery("SELECT ?publisher_name WHERE {<" + datasetURI + "> <" + DCTERMS.PUBLISHER + ">/<" + FOAF.NAME + "> ?publisher_name FILTER(LANGMATCHES(LANG(?publisher_name), \"" + configuration.getLoadLanguage() + "\"))}", "publisher_name");

            LinkedList<String> keywords = new LinkedList<>();
            for (Map<String, Value> map : executeSelectQuery("SELECT ?keyword WHERE {<" + datasetURI + "> <" + DcatAp11ToCkanBatchVocabulary.DCAT_KEYWORD + "> ?keyword FILTER(LANGMATCHES(LANG(?keyword), \"" + configuration.getLoadLanguage() + "\"))}")) {
                keywords.add(map.get("keyword").stringValue());
            }

            LinkedList<String> distributions = new LinkedList<>();
            for (Map<String, Value> map : executeSelectQuery("SELECT ?distribution WHERE {<" + datasetURI + "> <" + DcatAp11ToCkanBatchVocabulary.DCAT_DISTRIBUTION + "> ?distribution }")) {
                distributions.add(map.get("distribution").stringValue());
            }

            boolean exists = false;
            Map<String, String> resUrlIdMap = new HashMap<>();
            Map<String, String> resDistroIdMap = new HashMap<>();
            Map<String, JSONObject> resourceList = new HashMap<>();

            LOG.debug("Querying for the dataset in CKAN");
            HttpGet httpGet = new HttpGet(apiURI + "/package_show?id=" + datasetID);
            try {
                queryResponse = queryClient.execute(httpGet);
                if (queryResponse.getStatusLine().getStatusCode() == 200) {
                    LOG.debug("Dataset found");
                    exists = true;

                    JSONObject response = new JSONObject(EntityUtils.toString(queryResponse.getEntity())).getJSONObject("result");
                    JSONArray resourcesArray = response.getJSONArray("resources");
                    for (int i = 0; i < resourcesArray.length(); i++) {
                        try {
                            String id = resourcesArray.getJSONObject(i).getString("id");
                            resourceList.put(id, resourcesArray.getJSONObject(i));

                            String url = resourcesArray.getJSONObject(i).getString("url");
                            resUrlIdMap.put(url, id);

                            if (resourcesArray.getJSONObject(i).has("distro_url")) {
                                String distro = resourcesArray.getJSONObject(i).getString("distro_url");
                                resDistroIdMap.put(distro, id);
                            }
                        } catch (JSONException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                        }
                    }
                } else {
                    String ent = EntityUtils.toString(queryResponse.getEntity());
                    LOG.debug("Dataset not found: " + ent);
                }
            } catch (ClientProtocolException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (ParseException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (JSONException e) {
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

            if (!organizations.containsKey(publisher_uri)) {
                LOG.debug("Creating organization " + publisher_uri);
                JSONObject root = new JSONObject();

                if (publisher_name == null || publisher_name.isEmpty()) {
                    throw exceptionFactory.failed("Organization has no name: " + publisher_uri);
                }

                root.put("title", publisher_name);
                String orgname = Normalizer.normalize(publisher_name, Normalizer.Form.NFD)
                        .replaceAll("\\P{InBasic_Latin}", "")
                        .replace(' ', '-')
                        .replace('.', '-')
                        .toLowerCase();
                root.put("name", orgname);
                JSONArray org_extras = new JSONArray();
                org_extras.put(new JSONObject().put("key", "uri").put("value", publisher_uri));
                root.put("extras", org_extras);

                CloseableHttpClient client = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
                HttpPost httpPost = new HttpPost(apiURI + "/organization_create");
                httpPost.addHeader(new BasicHeader("Authorization", configuration.getApiKey()));

                String json = root.toString();

                httpPost.setEntity(new StringEntity(json, Charset.forName("utf-8")));

                CloseableHttpResponse response = null;

                try {
                    response = client.execute(httpPost);
                    if (response.getStatusLine().getStatusCode() == 200) {
                        LOG.debug("Organization created OK");
                        //LOG.info("Response: " + EntityUtils.toString(response.getEntity()));
                        organizations.put(publisher_uri, orgname);
                    } else if (response.getStatusLine().getStatusCode() == 409) {
                        String ent = EntityUtils.toString(response.getEntity());
                        LOG.error("Organization conflict: " + ent);
                        throw exceptionFactory.failed("Organization conflict: " + ent);
                    } else {
                        String ent = EntityUtils.toString(response.getEntity());
                        LOG.error("Response:" + ent);
                        throw exceptionFactory.failed("Error creating organization: " + ent);
                    }
                } catch (ClientProtocolException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                } finally {
                    if (response != null) {
                        try {
                            response.close();
                            client.close();
                        } catch (IOException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                            throw exceptionFactory.failed("Error creating dataset");
                        }
                    }
                }
            }

            LOG.debug("Creating JSON");

            JSONObject root = new JSONObject();

            JSONArray tags = new JSONArray();
            for (String keyword : keywords) {
                String safekeyword = keyword.replace(",","")
                        .replace(".","")
                        .replace("/","-")
                        .replace(":","-")
                        .replace(";","-")
                        .replace("§", "paragraf");
                if (safekeyword.length() < 2) {
                    continue;
                } else {
                    tags.put(new JSONObject().put("name", safekeyword));
                }
            }

            JSONArray resources = new JSONArray();

            if (!datasetID.isEmpty()) {
                root.put("name", datasetID);
            }
            if (!title.isEmpty()) {
                root.put("title", title);
            }
            if (!description.isEmpty()) {
                root.put("notes", description);
            }
            if (!contactPoint.isEmpty()) {
                root.put("maintainer_email", contactPoint);
            }
            if (!curatorName.isEmpty()) {
                root.put("maintainer", curatorName);
            }
            if (!issued.isEmpty()) {
                root.put("metadata_created", issued);
            }
            if (!modified.isEmpty()) {
                root.put("metadata_modified", modified);
            }

            //Distributions
            for (String distribution : distributions) {
                JSONObject distro = new JSONObject();

                String dtitle = executeSimpleSelectQuery("SELECT ?title WHERE {<" + distribution + "> <" + DCTERMS.TITLE + "> ?title FILTER(LANGMATCHES(LANG(?title), \"" + configuration.getLoadLanguage() + "\"))}", "title");
                String ddescription = executeSimpleSelectQuery("SELECT ?description WHERE {<" + distribution + "> <" + DCTERMS.DESCRIPTION + "> ?description FILTER(LANGMATCHES(LANG(?description), \"" + configuration.getLoadLanguage() + "\"))}", "description");
                String dissued = executeSimpleSelectQuery("SELECT ?issued WHERE {<" + distribution + "> <" + DCTERMS.ISSUED + "> ?issued }", "issued");
                String dmodified = executeSimpleSelectQuery("SELECT ?modified WHERE {<" + distribution + "> <" + DCTERMS.MODIFIED + "> ?modified }", "modified");
                String dwnld = executeSimpleSelectQuery("SELECT ?dwnld WHERE {<" + distribution + "> <" + DcatAp11ToCkanBatchVocabulary.DCAT_DOWNLOADURL + "> ?dwnld }", "dwnld");
                String access = executeSimpleSelectQuery("SELECT ?acc WHERE {<" + distribution + "> <" + DcatAp11ToCkanBatchVocabulary.DCAT_ACCESSURL + "> ?acc }", "acc");

                //hack
                if (dwnld == null || dwnld.isEmpty()) {
                    dwnld = access;
                    if (dwnld == null || dwnld.isEmpty()) {
                        LOG.warn("Empty download and access URLs: " + datasetURI);
                        continue;
                    }
                }

                if (!dtitle.isEmpty()) {
                    distro.put("name", dtitle);
                }
                if (!ddescription.isEmpty()) {
                    distro.put("description", ddescription);
                }
                if (!dwnld.isEmpty()) {
                    distro.put("url", dwnld);
                }
                if (!distribution.isEmpty()) {
                    distro.put("distro_url", distribution);
                }

                distro.put("resource_type", "file");

                if (resDistroIdMap.containsKey(distribution)) {
                    String id = resDistroIdMap.get(distribution);
                    distro.put("id", id);
                    resourceList.remove(id);
                } else if (resUrlIdMap.containsKey(dwnld)) {
                    String id = resUrlIdMap.get(dwnld);
                    distro.put("id", id);
                    resourceList.remove(id);
                }

                if (!dissued.isEmpty()) {
                    distro.put("created", dissued);
                }
                if (!dmodified.isEmpty()) {
                    distro.put("last_modified", dmodified);
                }

                resources.put(distro);
            }

            //Add the remaining distributions that were not updated but existed in the original dataset
            for (Entry<String, JSONObject> resource : resourceList.entrySet()) {
                resources.put(resource.getValue());
            }

            root.put("tags", tags);
            root.put("resources", resources);

            if (!exists) {
                JSONObject createRoot = new JSONObject();

                createRoot.put("name", datasetID);
                createRoot.put("title", title);

                createRoot.put("owner_org", organizations.get(publisher_uri));

                LOG.debug("Creating dataset in CKAN");
                HttpPost httpPost = new HttpPost(apiURI + "/package_create?id=" + datasetID);
                httpPost.addHeader(new BasicHeader("Authorization", configuration.getApiKey()));

                String json = createRoot.toString();

                LOG.debug("Creating dataset with: " + json);

                httpPost.setEntity(new StringEntity(json, Charset.forName("utf-8")));

                CloseableHttpResponse response = null;

                try {
                    response = createClient.execute(httpPost);
                    if (response.getStatusLine().getStatusCode() == 200) {
                        LOG.debug("Dataset created OK");
                        //LOG.info("Response: " + EntityUtils.toString(response.getEntity()));
                    } else if (response.getStatusLine().getStatusCode() == 409) {
                        String ent = EntityUtils.toString(response.getEntity());
                        LOG.error("Dataset already exists: " + ent);
                        throw exceptionFactory.failed("Dataset already exists");
                    } else {
                        String ent = EntityUtils.toString(response.getEntity());
                        LOG.error("Response:" + ent);
                        throw exceptionFactory.failed("Error creating dataset");
                    }
                } catch (ClientProtocolException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                } catch (IOException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                } finally {
                    if (response != null) {
                        try {
                            response.close();
                        } catch (IOException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                            throw exceptionFactory.failed("Error creating dataset");
                        }
                    }
                }
            }

            String json = root.toString();

            LOG.debug("Posting to CKAN");
            HttpPost httpPost = new HttpPost(apiURI + "/package_update?id=" + datasetID);
            httpPost.addHeader(new BasicHeader("Authorization", configuration.getApiKey()));

            LOG.debug(json);

            httpPost.setEntity(new StringEntity(json, Charset.forName("utf-8")));

            CloseableHttpResponse response = null;

            try {
                response = postClient.execute(httpPost);
                if (response.getStatusLine().getStatusCode() == 200) {
                    //LOG.info("Response:" + EntityUtils.toString(response.getEntity()));
                } else {
                    String ent = EntityUtils.toString(response.getEntity());
                    LOG.error("Response:" + ent);
                    throw exceptionFactory.failed("Error updating dataset");
                }
            } catch (ClientProtocolException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage(), e);
            } finally {
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException e) {
                        LOG.error(e.getLocalizedMessage(), e);
                        throw exceptionFactory.failed("Error updating dataset");
                    }
                }
            }

            progressReport.entryProcessed();
        }


        try {
            queryClient.close();
            createClient.close();
            postClient.close();
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }

        progressReport.done();
        
    }

    private String executeSimpleSelectQuery(final String queryAsString, String bindingName) throws LpException {
        return metadata.execute((connection) -> {
            final TupleQuery preparedQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryAsString);
            final SimpleDataset dataset = new SimpleDataset();
            dataset.addDefaultGraph(metadata.getGraph());
            preparedQuery.setDataset(dataset);
            //
            final BindingSet binding = QueryResults.singleResult(preparedQuery.evaluate());
            if (binding == null) {
                return "";
            } else {
                return binding.getValue(bindingName).stringValue();
            }
        });
    }

    private List<Map<String, Value>> executeSelectQuery(final String queryAsString) throws LpException {
        return metadata.execute((connection) -> {
            final List<Map<String, Value>> output = new LinkedList<>();
            final TupleQuery preparedQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryAsString);
            final SimpleDataset dataset = new SimpleDataset();
            dataset.addDefaultGraph(metadata.getGraph());
            preparedQuery.setDataset(dataset);
            //
            TupleQueryResult result = preparedQuery.evaluate();
            while (result.hasNext()) {
                final BindingSet binding = result.next();
                final Map<String, Value> row = new HashMap<>();
                binding.forEach((item) -> {
                    row.put(item.getName(), item.getValue());
                });
                output.add(row);
            }

            return output;
        });
    }

}
