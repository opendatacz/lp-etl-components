package cz.opendata.linked.cz.gov.nkod;

import com.linkedpipes.etl.dpu.api.rdf.RdfToPojo;

@RdfToPojo.Type(uri = "http://data.gov.cz/resource/lp/etl/components/e-nkod/Configuration")
public class ExtractorConfig  {

    @RdfToPojo.Property(uri = "http://data.gov.cz/resource/lp/etl/components/e-nkod/rewriteCache")
	private boolean rewriteCache;
    
    @RdfToPojo.Property(uri = "http://data.gov.cz/resource/lp/etl/components/e-nkod/interval")
    private int interval;

    public ExtractorConfig() {
    	
    }
    
    public boolean isRewriteCache() {
        return rewriteCache;
    }

    public void setRewriteCache(boolean rewriteCache) {
        this.rewriteCache = rewriteCache;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

}
