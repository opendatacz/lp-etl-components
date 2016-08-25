package com.linkedpipes.plugin.transformer.fdp;

import com.linkedpipes.etl.dataunit.sesame.api.rdf.SingleGraphDataUnit;
import com.linkedpipes.etl.dataunit.sesame.api.rdf.WritableSingleGraphDataUnit;
import com.linkedpipes.etl.dataunit.system.api.files.FilesDataUnit;
import com.linkedpipes.etl.dataunit.system.api.files.FilesDataUnit.Entry;

import com.linkedpipes.plugin.transformer.fdp.dimension.*;
import org.openrdf.model.*;
import org.openrdf.query.*;
import org.openrdf.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import com.linkedpipes.etl.component.api.Component;
import com.linkedpipes.etl.component.api.service.ExceptionFactory;
import com.linkedpipes.etl.executor.api.v1.exception.LpException;

import org.openrdf.query.impl.SimpleDataset;

/**
 *
 * @author Škoda Petr
 */
public final class FdpToRdf implements Component.Sequential {
	
	private static final ValueFactory VALUE_FACTORY
    = SimpleValueFactory.getInstance();

    private static final Logger LOG
            = LoggerFactory.getLogger(FdpToRdf.class);

    @Component.InputPort(id = "InputRdf")
    public SingleGraphDataUnit inputRdf;

    @Component.InputPort(id = "InputFiles")
    public FilesDataUnit inputFilesDataUnit;

    @Component.InputPort(id = "OutputRdf")
    public WritableSingleGraphDataUnit outputRdf;

    @Component.Configuration
    public FdpToRdfConfiguration configuration;

    @Component.Inject
    public ExceptionFactory exceptionFactory;
    
    private BufferedOutput output;
    
    private List<FdpDimension> dimensions = new ArrayList<FdpDimension>();
    private List<HierarchicalDimension> hierarchicalDimensions = new ArrayList<HierarchicalDimension>();
    private List<FdpMeasure> measures = new ArrayList<FdpMeasure>();
    
    private List<BindingSet> currentResult = null;
    
    public void storeCurrentResult(List<BindingSet> result) {
    	currentResult = result;
    }

    private String datasetIRI;
    
    private List<BindingSet> execQuery(String queryText) throws LpException {
    	try{
    	inputRdf.execute((connection) -> {
        	
    		//String queryText = "PREFIX fdprdf: <http://data.openbudgets.eu/fdptordf#> SELECT ?attribute ?valueProp ?parentDimension WHERE { ?attribute fdprdf:valueProperty ?valueProp; fdprdf:parentDimension ?parentDimension . }";
        
            final TupleQuery query = connection.prepareTupleQuery(
            		QueryLanguage.SPARQL, queryText);
            final SimpleDataset dataset = new SimpleDataset();
            final IRI inputGraph = inputRdf.getGraph();
            dataset.addDefaultGraph(inputGraph);
            // We need to add this else we can not use
            // GRAPH ?g in query.
            dataset.addNamedGraph(inputGraph);
            query.setDataset(dataset);
            final TupleQueryResult result = query.evaluate();
            final List<BindingSet> resultBindings = new ArrayList<BindingSet>();
            while (result.hasNext()) {
                resultBindings.add(result.next());
            }
            storeCurrentResult(resultBindings);
    	});
    	}
    	catch(Exception ex) {
    		 throw exceptionFactory.failed("Can't extract metadata, the failed query was: \r\n {}", queryText);
    	}
    	return currentResult;
    }

    private void extractDataset() throws LpException {
        execQuery(FdpMeasure.query);
        if(currentResult.size() == 0) throw exceptionFactory.failed("Dataset IRI not found in metadata.");
        Binding datasetBinding = currentResult.get(0).getBinding("dataset");
        if(datasetBinding == null) throw exceptionFactory.failed("Dataset IRI not found in metadata.");
        datasetIRI = datasetBinding.getValue().stringValue();
    }
    
    private void extractDimensions() throws LpException {

       execQuery(MultiAttributeDimension.dimensionQuery);
       for(BindingSet bs : currentResult) {
            MultiAttributeDimension dim = new MultiAttributeDimension();
            initDimension(dim, bs);
            dimensions.add(dim);
            //output.submit(valueProp, VALUE_FACTORY.createIRI("http://hasParent"), parentDimension);
        }

        execQuery(SkosDimension.dimensionQuery);
        for(BindingSet bs : currentResult) {
            SkosDimension dim = new SkosDimension();
            initDimension(dim, bs);
            dimensions.add(dim);
        }

        execQuery(HierarchicalDimension.dimensionQuery);
        for(BindingSet bs : currentResult) {
            HierarchicalDimension dim = new HierarchicalDimension();
            initDimension(dim, bs);
            hierarchicalDimensions.add(dim);
        }

        execQuery(SingleAttributeObjectDimension.dimensionQuery);
        for(BindingSet bs : currentResult) {
            SingleAttributeObjectDimension dim = new SingleAttributeObjectDimension();
            initDimension(dim, bs);
            dimensions.add(dim);
        }

        execQuery(SingleAttributeLiteralDimension.dimensionQuery);
        for(BindingSet bs : currentResult) {
            SingleAttributeLiteralDimension dim = new SingleAttributeLiteralDimension();
            initDimension(dim, bs);
            dimensions.add(dim);
        }

        execQuery(DateDimension.dimensionQuery);
        for(BindingSet bs : currentResult) {
            DateDimension dim = new DateDimension();
            initDimension(dim, bs);
            dimensions.add(dim);
        }

        execQuery(SingleAttributeSkosDimension.dimensionQuery);
        for(BindingSet bs : currentResult) {
            SingleAttributeSkosDimension dim = new SingleAttributeSkosDimension();
            initDimension(dim, bs);
            dimensions.add(dim);
        }
    }

    private void extractMeasures() throws LpException {
        execQuery(FdpMeasure.query);
        for(BindingSet bs : currentResult) {
            measures.add(new FdpMeasure(output,
                    ((Literal)bs.getBinding("measureFactor").getValue()).doubleValue(),
                    bs.getBinding("measureProperty").getValue().stringValue(),
                    bs.getBinding("sourceColumn").getValue().stringValue(),
                    bs.getBinding("sourceFile").getValue().stringValue()));
        }
    }
    
    private void extractAttributes() throws LpException {
    	for(FdpDimension dim : dimensions) {
    		List<FdpAttribute> attributes = new ArrayList<FdpAttribute>();
    		execQuery(dim.getAttributeQuery());
    		for(BindingSet bs : currentResult) {
                FdpAttribute attr = new FdpAttribute(
                		bs.getBinding("sourceColumn").getValue().stringValue(),
                		bs.getBinding("sourceFile").getValue().stringValue(),
                		((Literal) bs.getBinding("iskey").getValue()).booleanValue(),
                		(Resource) bs.getBinding("attributeValueProperty").getValue());
                attributes.add(attr);
    		}
    		dim.setAttributes(attributes);                
    	}
        for(HierarchicalDimension dim : hierarchicalDimensions) {
            List<FdpAttribute> attributes = new ArrayList<FdpAttribute>();
            execQuery(dim.getAttributeQuery());
            for(BindingSet bs : currentResult) {
                FdpHierarchicalAttribute attr = new FdpHierarchicalAttribute(
                        bs.getBinding("sourceColumn").getValue().stringValue(),
                        bs.getBinding("sourceFile").getValue().stringValue(),
                        ((Literal) bs.getBinding("iskey").getValue()).booleanValue(),
                        (Resource) bs.getBinding("attributeValueProperty").getValue(),
                        bs.getBinding("attributeName").getValue().stringValue());
                if(bs.getBinding("parentName")!=null) attr.setParent(bs.getBinding("parentName").getValue().stringValue());
                attributes.add(attr);
            }
            dim.setAttributes(attributes);

            execQuery(dim.getLabelsQuery());
            for(BindingSet bs : currentResult) {
                dim.addLabel(bs.getBinding("labelForName").getValue().stringValue(), bs.getBinding("sourceColumn").getValue().stringValue());
            }
        }

    }
    
    private void initDimension(FdpDimension dimension, BindingSet bs) {
        IRI valueProp = (IRI) bs.getBinding("dimensionProp").getValue();
        String dimName = (String) bs.getBinding("dimensionName").getValue().stringValue();
        String datasetIri = (String) bs.getBinding("dataset").getValue().stringValue();
        String datasetName = (String) bs.getBinding("packageName").getValue().stringValue();
        dimension.init(output, valueProp, dimName, datasetIri, datasetName);
        if(bs.getBinding("rdfType")!=null) dimension.setValueType((IRI) bs.getBinding("rdfType").getValue());
    }

    @Override
    public void execute() throws LpException {
        
        output = new BufferedOutput(outputRdf);
        final Parser parser = new Parser(exceptionFactory);
        // TODO We could use some table group URI from user?

        extractDataset();
        extractDimensions();
        extractAttributes();
        extractMeasures();
        List<FdpDimension> allDimensions = new ArrayList<FdpDimension>();
        allDimensions.addAll(dimensions);
        allDimensions.addAll(hierarchicalDimensions);
        final Mapper mapper = new Mapper(output, exceptionFactory, allDimensions, measures, datasetIRI);
        
        output.onFileStart();

        if(inputFilesDataUnit.size() > 1) throw exceptionFactory.failed("Only one CSV file is supported at the moment.");
        for (Entry entry : inputFilesDataUnit) {
            LOG.info("Processing file: {}", entry.toFile());
            try {
                parser.parse(entry, mapper);
            } catch (Exception ex) {
                throw exceptionFactory.failed("Can't process file: {}",
                        entry.getFileName(), ex);
            }
            mapper.onTableEnd();
            output.onFileEnd();
        }
    }

}
