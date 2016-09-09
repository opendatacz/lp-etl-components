package com.linkedpipes.plugin.transformer.fdp;

public final class FdpToRdfVocabulary {

    private static final String PREFIX
            = "http://plugins.linkedpipes.com/ontology/t-fdpToRdf#";

    public static final String CONFIG = PREFIX + "Configuration";

    public static final String HAS_FILE_NAME = PREFIX + "fileName";

    public static final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String SKOS = "http://www.w3.org/2004/02/skos/core#";
    public static final String SCHEMA = "http://schema.org/";
    public static final String TIME = "http://www.w3.org/2006/time#";
    public static final String QB = "http://purl.org/linked-data/cube#";

    public static final String A = RDF + "type";
    public static final String SKOS_CONCEPT = SKOS + "Concept";
    public static final String SKOS_PREFLABEL = SKOS + "prefLabel";
    public static final String SKOS_INSCHEME = SKOS + "inScheme";
    public static final String SKOS_CONCEPTSCHEME = SKOS + "ConceptScheme";
    public static final String SKOS_HASTOPCONCEPT = SKOS + "hasTopConcept";
    public static final String SKOS_NOTATION = SKOS + "notation";
    public static final String SKOS_BROADER = SKOS + "broader";

    public static final String SCHEMA_NAME = SCHEMA + "name";

    public static final String TIME_INTERVAL = TIME + "Interval";

    public static final String QB_OBSERVATION = QB + "observation";
    public static final String QB_OBSERVATION_TYPE = QB + "Observation";
    public static final String QB_DATASET = QB + "dataset";

    private FdpToRdfVocabulary() {
    }

}
