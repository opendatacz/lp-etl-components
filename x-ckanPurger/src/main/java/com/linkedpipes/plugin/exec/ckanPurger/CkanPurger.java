package com.linkedpipes.plugin.exec.ckanPurger;

import com.linkedpipes.etl.dataunit.core.files.FilesDataUnit;
import com.linkedpipes.etl.executor.api.v1.LpException;
import com.linkedpipes.etl.executor.api.v1.component.Component;
import com.linkedpipes.etl.executor.api.v1.component.SequentialExecution;
import com.linkedpipes.etl.executor.api.v1.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.service.ProgressReport;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CkanPurger implements Component, SequentialExecution {

    private static final Logger LOG = LoggerFactory.getLogger(CkanPurger.class);

    @Component.InputPort(iri = "InputFiles")
    public FilesDataUnit inputFiles;

    @Component.Configuration
    public CkanPurgerConfiguration configuration;

    @Component.Inject
    public ExceptionFactory exceptionFactory;

    @Component.Inject
    public ProgressReport progressReport;

    private CkanManager ckanManager;

    @Override
    public void execute() throws LpException {
        checkConfiguration();
        Ckan ckan = new Ckan(configuration.getApiUri(),
                configuration.getApiKey());
        HttpRequestExecutor executor = new HttpRequestExecutor();
        try {
            ckanManager = new CkanManager(executor, ckan);
            List<String> datasets = getDatasetsToPurge();
            List<String> organizations = getOrganizationsToPurge();
            progressReport.start(datasets.size() + organizations.size());
            purgeDatasets(datasets);
            purgeOrganizations(organizations);
            progressReport.done();
        } catch (CkanManager.CkanException ex) {
            throw exceptionFactory.failure("CKAN operation failed.", ex);
        } finally {
            executor.close();
        }
    }

    private void checkConfiguration() throws LpException {
        if (configuration.getApiUri() == null
                || configuration.getApiUri().isEmpty()) {
            throw exceptionFactory.failure("Missing apiUrl.");
        }
        if (configuration.getApiKey() == null
                || configuration.getApiKey().isEmpty()) {
            throw exceptionFactory.failure("Missing apiKey.");
        }
    }

    private List<String> getDatasetsToPurge()
            throws CkanManager.CkanException, LpException {
        if (configuration.isPurgeAllDatasets()) {
            return ckanManager.getDatasets();
        } else {
            return readDatasetsFromInputFiles();
        }
    }

    private List<String> readDatasetsFromInputFiles() throws LpException {
        List<String> datasets = new LinkedList<>();
        for (FilesDataUnit.Entry entry : inputFiles) {
            datasets.addAll(readDatasetsFromFile(entry));
        }
        return datasets;
    }

    private List<String> readDatasetsFromFile(FilesDataUnit.Entry entry)
            throws LpException {
        try {
            return CsvReader.readColumn(entry.toFile(), 0);
        } catch (IOException ex) {
            throw exceptionFactory.failure("Can't read file: {}",
                    entry.getFileName(), ex);
        }
    }

    private List<String> getOrganizationsToPurge()
            throws CkanManager.CkanException {
        if (configuration.isPurgeAllOrganizations()) {
            return ckanManager.getOrganizations();
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    private void purgeDatasets(List<String> datasets) throws LpException {
        int counter = 0;
        for (String dataset : datasets) {
            LOG.info("Purging dataset " + counter + "/" + datasets.size()
                    + " : " + dataset);
            counter++;
            try {
                ckanManager.deleteDataset(dataset);
            } catch (CkanManager.CkanException ex) {
                handleException(exceptionFactory.failure(
                        "Can't delete dataset: {}", dataset, ex));
            }
        }
    }

    private void purgeOrganizations(List<String> organizations)
            throws LpException {
        int counter = 0;
        for (String organization : organizations) {
            LOG.info("Purging organization " + counter + "/"
                    + organizations.size() + " : " + organization);
            counter++;
            try {
                ckanManager.deleteOrganization(organization);
            } catch (CkanManager.CkanException ex) {
                handleException(exceptionFactory.failure(
                        "Can't delete organization: {}", organization, ex));
            }
        }
    }

    private void handleException(LpException exception) throws LpException {
        if (configuration.isFailOnError()) {
            throw exception;
        } else {
            LOG.error("Operation failed.", exception);
        }
    }

}
