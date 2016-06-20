package cz.opendata.linked.cz.gov.nkod;

import com.linkedpipes.etl.component.api.service.RdfToPojo;

@RdfToPojo.Type(uri = "http://data.gov.cz/resource/lp/etl/components/e-portal-gov-cz/Configuration")
public class ExtractorConfig  {

    @RdfToPojo.Property(uri = "http://data.gov.cz/resource/lp/etl/components/e-portal-gov-cz/rewriteCache")
	private boolean rewriteCache;
    
    @RdfToPojo.Property(uri = "http://data.gov.cz/resource/lp/etl/components/e-portal-gov-cz/interval")
    private int interval;

    @RdfToPojo.Property(uri = "http://data.gov.cz/resource/lp/etl/components/e-portal-gov-cz/registry")
	private int registry;

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

	public int getRegistry() {
		return registry;
	}

	public void setRegistry(int registry) {
		this.registry = registry;
	}

}
