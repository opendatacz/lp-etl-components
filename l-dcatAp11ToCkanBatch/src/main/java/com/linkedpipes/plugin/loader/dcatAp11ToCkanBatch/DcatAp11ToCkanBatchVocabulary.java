package com.linkedpipes.plugin.loader.dcatAp11ToCkanBatch;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class DcatAp11ToCkanBatchVocabulary {

    public static final String SCHEMA = "http://schema.org/";

    public static final String VOID = "http://rdfs.org/ns/void#";

    public static final String XSD = "http://www.w3.org/2001/XMLSchema#";

    public static final String POD = "https://project-open-data.cio.gov/v1.1/schema/#";

    public static final String WDRS = "http://www.w3.org/2007/05/powder-s#";

    public static final String VCARD = "http://www.w3.org/2006/vcard/ns#";

    public static final String LODCZCKAN = "http://linked.opendata.cz/ontology/ckan/";

    public static final String PROFILES = "http://plugins.etl.linkedpipes.com/resource/l-dcatAp11ToCkanBatch/profiles/";

    public static final IRI VOID_DATASET_CLASS;

    public static final IRI VOID_EXAMPLERESOURCE;

    public static final IRI VOID_DATADUMP;

    public static final IRI VOID_SPARQLENDPOINT;

    public static final IRI SCHEMA_ENDDATE;

    public static final IRI SCHEMA_STARTDATE;

    public static final IRI POD_DISTRIBUTION_DESCRIBREBYTYPE;

    public static final IRI WDRS_DESCRIBEDBY;

    public static final IRI XSD_DATE;

    public static final IRI VCARD_VCARD_CLASS;

    public static final IRI VCARD_HAS_EMAIL;

    public static final IRI VCARD_FN;

    public static final IRI LODCZCKAN_DATASET_ID;

    public static final IRI PROFILES_CKAN;

    public static final IRI PROFILES_NKOD;

    static {
        final SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

        VOID_DATASET_CLASS = valueFactory.createIRI(VOID + "Dataset");
        VOID_EXAMPLERESOURCE = valueFactory.createIRI(VOID + "exampleResource");
        VOID_DATADUMP = valueFactory.createIRI(VOID + "dataDump");
        VOID_SPARQLENDPOINT = valueFactory.createIRI(VOID + "sparqlEndpoint");
        XSD_DATE = valueFactory.createIRI(XSD + "date");
        SCHEMA_ENDDATE = valueFactory.createIRI(SCHEMA + "endDate");
        SCHEMA_STARTDATE = valueFactory.createIRI(SCHEMA + "startDate");
        POD_DISTRIBUTION_DESCRIBREBYTYPE = valueFactory.createIRI(POD + "distribution-describedByType");
        WDRS_DESCRIBEDBY = valueFactory.createIRI(WDRS + "describedBy");
        VCARD_VCARD_CLASS = valueFactory.createIRI(VCARD + "VCard");
        VCARD_HAS_EMAIL = valueFactory.createIRI(VCARD + "hasEmail");
        VCARD_FN = valueFactory.createIRI(VCARD + "fn");
        LODCZCKAN_DATASET_ID = valueFactory.createIRI(LODCZCKAN + "datasetID");

        PROFILES_CKAN = valueFactory.createIRI(PROFILES + "CKAN");
        PROFILES_NKOD = valueFactory.createIRI(PROFILES + "CZ-NKOD");
    }

}
