package com.linkedpipes.plugin.transformer.fdp;

import org.openrdf.model.IRI;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by admin on 2.9.2016.
 */
public class PlainTextTripleWriter {

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
    }

    public void submit(Resource subject, IRI predicate, Literal object) throws IOException {
        if( object.getDatatype().getLocalName().compareToIgnoreCase("string") == 0 ) {
            writeTriple(subjectPredicatePart(subject, predicate), stringLiteralToString(object));
        }
        else writeTriple(subjectPredicatePart(subject, predicate), object.stringValue());
    }

    public void onFileEnd() throws IOException {
        outWriter.close();
    }
}
