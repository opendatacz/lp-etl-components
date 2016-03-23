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

        try (final TestEnvironment env = TestEnvironment.create(dpu, TestUtils.getTempDirectory())) {
            
            final WritableFilesDataUnit output = env.bindSystemDataUnit("OutputFiles", new File(TestUtils.getTempDirectory() + "/files/"));
            final WritableFilesDataUnit roky = env.bindSystemDataUnit("OutputRoky", new File(TestUtils.getTempDirectory() + "/roky/"));
            final WritableSingleGraphDataUnit metadata = env.bindSingleGraphDataUnit("Metadata");
            //
            env.execute();
            TestUtils.store(metadata, new File("C://Tools//xsltmetadata.ttl"), RDFFormat.TURTLE);
        } catch (Exception ex) {
            LOG.error("Failure", ex);
        }
    }

}
