package com.linkedpipes.plugin.transformer.fdp;


import java.io.IOException;
import java.io.OutputStreamWriter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Created by admin on 2.9.2016.
 */
public class PlainTextTripleWriter {

    private final static ValueFactory VALUE_FACTORY
            = SimpleValueFactory.getInstance();

    OutputStreamWriter outWriter;

    public PlainTextTripleWriter(OutputStreamWriter writer){
        this.outWriter = writer;
    }

    private String subjectPredicatePart(Resource subject, IRI predicate) {
        return "<"+subject.stringValue()+"> <"+predicate.stringValue()+"> ";
    }

    private String stringLiteralToString(Value literal) {
        return "\""+literal.stringValue()+"\"";
    }

    private String bracketedUri(Value object) { return "<"+object.stringValue()+">";}

    private void writeTriple(String subjPredString, String objString) throws IOException {
        String tripleString = subjPredString + objString + " .\r\n";
        outWriter.write(tripleString);
    }

    public void submit(Resource subject, IRI predicate, Value object) throws IOException {
        writeTriple(subjectPredicatePart(subject,predicate), bracketedUri(object));

        //Statement statement = VALUE_FACTORY.createStatement(subject, predicate, object);
        //outWriter.write(statement.getObject().toString()+"\r\n");
    }

    public void submit(Resource subject, IRI predicate, Literal object) throws IOException {
        writeTriple(subjectPredicatePart(subject, predicate), object.toString());

        /*
        if( object.getDatatype().getLocalName().compareToIgnoreCase("string") == 0 ) {

        }
        else writeTriple(subjectPredicatePart(subject, predicate), object.stringValue());*/
    }

    public void onFileEnd() throws IOException {
        outWriter.flush();
    }
}
