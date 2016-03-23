package cz.opendata.linked.cz.gov.nkod;

import org.openrdf.model.IRI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;

/**
 *
 * @author Klímek Jakub
 */
public class ExtractorOntology {

    /**
     * Class for the main configuration object.
     */
    public static final IRI XSLT_CLASS;

    /**
     * Class for the XSLT file info object.
     */
    public static final IRI XSLT_FILEINFO_CLASS;

    /**
     * Class for a single XSLT parameter.
     */
    public static final IRI XSLT_PARAM_CLASS;

    /**
     * Predicate to associate main configuration object with a file info object
     */
    public static final IRI XSLT_FILEINFO_PREDICATE;

    /**
     * Predicate to associate a file info object a file name
     */
    public static final IRI XSLT_FILEINFO_FILENAME_PREDICATE;

    /**
     * Predicate to associate a file info object with an XSLT parameter.
     */
    public static final IRI XSLT_FILEINFO_PARAM_PREDICATE;

    /**
     * XSLT parameter's name
     */
    public static final IRI XSLT_PARAM_NAME_PREDICATE;

    /**
     * XSLT parameter's value
     */
    public static final IRI XSLT_PARAM_VALUE_PREDICATE;

    static {
        final ValueFactory valueFactory = SimpleValueFactory.getInstance();

        XSLT_CLASS = valueFactory.createIRI("http://etl.linkedpipes.com/ontology/components/t-xslt/Config");

        XSLT_FILEINFO_CLASS = valueFactory
                .createIRI("http://etl.linkedpipes.com/ontology/components/t-xslt/FileInfo");

        XSLT_FILEINFO_PREDICATE = valueFactory.createIRI(
                "http://etl.linkedpipes.com/ontology/components/t-xslt/fileInfo");

        XSLT_FILEINFO_FILENAME_PREDICATE = valueFactory.createIRI(
                "http://etl.linkedpipes.com/ontology/components/t-xslt/fileName");

        XSLT_PARAM_CLASS = valueFactory.createIRI("http://etl.linkedpipes.com/ontology/components/t-xslt/Parameter");

        XSLT_FILEINFO_PARAM_PREDICATE = valueFactory.createIRI(
                "http://etl.linkedpipes.com/ontology/components/t-xslt/parameter");

        XSLT_PARAM_NAME_PREDICATE = valueFactory.createIRI(
                "http://etl.linkedpipes.com/ontology/components/t-xslt/parameterName");

        XSLT_PARAM_VALUE_PREDICATE = valueFactory.createIRI(
                "http://etl.linkedpipes.com/ontology/components/t-xslt/parameterValue");

    }

}
