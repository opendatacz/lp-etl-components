package cz.opendata.linked.cz.gov.nkod;

import java.io.File;

import org.junit.Test;
import org.openrdf.rio.RDFFormat;

import com.linkedpipes.etl.dataunit.sesame.api.rdf.WritableSingleGraphDataUnit;
import com.linkedpipes.etl.dataunit.system.api.files.WritableFilesDataUnit;
import com.linkedpipes.etl.dpu.test.TestEnvironment;
import com.linkedpipes.etl.dpu.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Klímek Jakub
 */
public class ExtractorTest {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractorTest.class);

    @Test
    public void transformJsonLd() throws Exception {
        final Extractor dpu = new Extractor();
        dpu.config = new ExtractorConfig();
        dpu.config.setRewriteCache(true);
        dpu.config.setRegistry(97898);
        dpu.config.setInterval(0);

        try (final TestEnvironment env = TestEnvironment.create(dpu, TestUtils.getTempDirectory())) {
            
            final WritableFilesDataUnit output = env.bindSystemDataUnit("Files", new File(TestUtils.getTempDirectory() + "/files/"));
            final WritableFilesDataUnit roky = env.bindSystemDataUnit("Indices", new File(TestUtils.getTempDirectory() + "/indices/"));
            final WritableFilesDataUnit vazby = env.bindSystemDataUnit("Relations", new File(TestUtils.getTempDirectory() + "/relations/"));
            final WritableSingleGraphDataUnit metadata = env.bindSingleGraphDataUnit("XSLTParameters");
            //
            env.execute();
            TestUtils.store(metadata, new File("xsltmetadata.ttl"), RDFFormat.TURTLE);
        } catch (Exception ex) {
            LOG.error("Failure", ex);
        }
    }

}
