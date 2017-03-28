package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.executor.api.v1.LpException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;


/**
 * Consumes triples and store then into output.
 */
public interface StatementConsumer {

    public void onRowStart();

    public void onRowEnd() throws LpException;

    public void onFileStart() throws LpException;

    public void onFileEnd() throws LpException;

    public void submit(Resource subject, IRI predicate, Value object)
            throws LpException;

}
