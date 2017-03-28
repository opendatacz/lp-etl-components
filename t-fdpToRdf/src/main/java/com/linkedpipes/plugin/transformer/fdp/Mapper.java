package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.executor.api.v1.LpException;
import com.linkedpipes.etl.executor.api.v1.service.ExceptionFactory;
import java.io.IOException;
import java.util.*;

import com.linkedpipes.plugin.transformer.fdp.dimension.FdpDimension;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * https://www.w3.org/TR/2015/REC-csv2rdf-20151217/#bib-tabular-data-model
 */
public class Mapper {

    /**
     * Valued factory used to create RDF data.
     */
    public static final ValueFactory VALUE_FACTORY
            = SimpleValueFactory.getInstance();

    /**
     * Used output consumer.
     */
    private final PlainTextTripleWriter consumer;

    /**
     * Row number in source file.
     */
    private int rowNumber = 0;

    /**
     * Processed row number.
     */
    private int processedRowNumber = 0;

    /**
     * Columns as configured by the user.
     */
    private List<String> columns;
    private String datasetIri;

    private final ExceptionFactory exceptionFactory;

    private List<FdpDimension> dimensions;
    private List<FdpMeasure> measures;

    /**
     * Does not call any other method on {@link StatementConsumer}
     * than onRowStart, onRowEnd and submit for new statement.
     *
     * @param consumer
     * @param columns
     */
    Mapper(PlainTextTripleWriter consumer, ExceptionFactory exceptionFactory, List<FdpDimension> dimensions,
           List<FdpMeasure> measures, String datasetIri) {
        this.consumer = consumer;
        this.exceptionFactory = exceptionFactory;
        this.dimensions = dimensions;
        this.measures = measures;
        this.datasetIri = datasetIri;
    }

    private IRI getObservationUri() {
        return VALUE_FACTORY.createIRI(datasetIri+"/observation/"+rowNumber);
    }

    /**
     * Must be called before {@link #onRow(java.util.List)}.
     *
     * @param header Null if there is no header.
     */
    public void onHeader(List<String> header) {
        columns = header;
    }

    /**
     *
     * @param row Row from the CSV file.
     * @return True if next line should be processed if it exists.
     */
    public boolean onRow(List<String> row) throws IOException,
            LpException {
        rowNumber++;

        processedRowNumber++;
        int i=0;
        HashMap<String, String> rowHash = new HashMap<String, String>();
        for(String rowValue : row) {
        	//consumer.submit(VALUE_FACTORY.createIRI("http://row/"+rowNumber),
        	//	VALUE_FACTORY.createIRI("http://column/"+columns.get(i)),
        	//	VALUE_FACTORY.createLiteral(rowValue));
        	rowHash.put(columns.get(i), rowValue);
        	i++;
        }
        for(FdpDimension dim : dimensions) {
        	dim.processRow(getObservationUri(), rowHash, exceptionFactory);
        }
        for(FdpMeasure measure : measures) {
            measure.processRow(getObservationUri(), rowHash, exceptionFactory);
        }

            consumer.submit(VALUE_FACTORY.createIRI(datasetIri), VALUE_FACTORY.createIRI(FdpToRdfVocabulary.QB_OBSERVATION),
                    getObservationUri());
            consumer.submit(getObservationUri(), VALUE_FACTORY.createIRI(FdpToRdfVocabulary.A),
                    VALUE_FACTORY.createIRI(FdpToRdfVocabulary.QB_OBSERVATION_TYPE));
            consumer.submit(getObservationUri(), VALUE_FACTORY.createIRI(FdpToRdfVocabulary.QB_DATASET),
                    VALUE_FACTORY.createIRI(datasetIri));

        return true;
    }

    public void onTableEnd() {
    }

}
