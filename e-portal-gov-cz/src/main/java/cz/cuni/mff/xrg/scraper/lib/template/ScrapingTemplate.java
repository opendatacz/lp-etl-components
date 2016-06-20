/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cuni.mff.xrg.scraper.lib.template;

import cz.cuni.mff.xrg.scraper.css_parser.utils.BannedException;
import cz.cuni.mff.xrg.scraper.css_parser.utils.Cache;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedList;

import org.slf4j.Logger;

import com.linkedpipes.etl.component.api.Component;


/**
 * Abstract class of common scraper.
 * 
 * The scraping process is done in following steps:
 *  - parse with doc type has to be called on initial URL
 *  - getLinks is called on initial URL with given docType
 *  - found links are added to queue for future parsing
 *  - parse method is called on initial URL with given docType (and do whathever implemented)
 * 
 * @author Jakub Starka
 */
public abstract class ScrapingTemplate {
    
    public Logger logger;
    
    /** 
     * This method looks for links in actual document and create entries with URL and document type.
     * 
     * @param doc Input JSoup document.
     * @param docType Textual name of input document (i.e. initial page, list page, detail page, ...
     * @return List of entries for additional scraping.
     */
    protected abstract LinkedList<ParseEntry> getLinks(String doc, String docType);
    
    /**
     * This method parses given document with document type and do something
     * 
     * @param doc Input JSoup document.
     * @param docType Textual name of input document (i.e. initial page, list page, detail page, ...
     */
    protected abstract void parse(String doc, String docType, URL uri);    
    
    /**
     * Run scraping on given URL and given document type.
     * 
     * @param initUrl Initial URL.
     * @param type Initial document type.
     * @throws InterruptedException 
     */
    public void parse(URL initUrl, String type) throws InterruptedException, BannedException {
        LinkedList<ParseEntry> toParse = new LinkedList<>();
        HashSet<ParseEntry> parsed = new HashSet<>();
        toParse.add(new ParseEntry(initUrl, type, "xml"));
        
        while (!toParse.isEmpty()) {
            try {
                ParseEntry p = toParse.pop();
                // skip if parsed
                if (parsed.contains(p)) {
                    continue;
                }
                String doc = Cache.getDocument(p.url, 10, p.datatype);
                if (doc!= null) {
                    toParse.addAll(this.getLinks(doc, p.type));
                    this.parse(doc, p.type, p.url);
                }
                if (Cache.errorsFetchingURL > 10) throw new BannedException();
                parsed.add(p);
            } catch (IOException ex) {
                logger.error(ex.getLocalizedMessage());
            } 
        }
        
    }
    
}
