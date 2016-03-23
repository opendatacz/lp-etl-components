package cz.opendata.linked.cz.gov.nkod;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedpipes.etl.dataunit.sesame.api.rdf.WritableSingleGraphDataUnit;
import com.linkedpipes.etl.dataunit.system.api.files.WritableFilesDataUnit;
import com.linkedpipes.etl.dpu.api.DataProcessingUnit;
import com.linkedpipes.etl.dpu.api.executable.SequentialExecution;
import com.linkedpipes.etl.executor.api.v1.exception.NonRecoverableException;

import cz.cuni.mff.xrg.scraper.css_parser.utils.BannedException;
import cz.cuni.mff.xrg.scraper.css_parser.utils.Cache;

public class Extractor implements SequentialExecution {

    private static final Logger LOG = LoggerFactory.getLogger(Extractor.class);

    @DataProcessingUnit.OutputPort(id = "OutputFiles")
    public WritableFilesDataUnit outNkodFiles;

    @DataProcessingUnit.OutputPort(id = "OutputRoky")
    public WritableFilesDataUnit outNkodRokyFiles;

    @DataProcessingUnit.OutputPort(id = "XSLTParameters")
    public WritableSingleGraphDataUnit outRdfMetadata;

    @DataProcessingUnit.Configuration
    public ExtractorConfig config;
    
    @Override
    public void execute(DataProcessingUnit.Context context) throws NonRecoverableException {
        Cache.setInterval(config.getInterval());
        Cache.setBaseDir(context.getWorkingDirectory() + "/cache/");
        Cache.logger = LOG;
        Cache.rewriteCache = config.isRewriteCache();
        Scraper_parser s = new Scraper_parser();
        s.logger = LOG;
        s.context = context;
        s.nkod = outNkodFiles;
        s.nkod_roky = outNkodRokyFiles;
        s.metadata = outRdfMetadata;

        java.util.Date date = new java.util.Date();
        long start = date.getTime();

        //Download
        try {
            URL init_nkod = new URL("http://portal.gov.cz/portal/rejstriky/data/97898/index.xml");

            if (config.isRewriteCache()) {
                Path path_nkod = Paths.get(
                        context.getWorkingDirectory() + "/cache/portal.gov.cz/portal/rejstriky/data/97898/index.xml");
                LOG.info("Deleting " + path_nkod);
                Files.deleteIfExists(path_nkod);
            }

            try {
                s.parse(init_nkod, "init-s");
            } catch (BannedException b) {
                LOG.warn("Seems like we are banned for today");
            }

            LOG.info("Download done.");

        } catch (IOException e) {
            LOG.error("IOException", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted");
        }

        java.util.Date date2 = new java.util.Date();
        long end = date2.getTime();

        LOG.info("Processed " + s.numNkod + " nkod from " + s.numNkodRoks + " years.");
        LOG.info("Processed in " + (end - start) + "ms");

    }
}
