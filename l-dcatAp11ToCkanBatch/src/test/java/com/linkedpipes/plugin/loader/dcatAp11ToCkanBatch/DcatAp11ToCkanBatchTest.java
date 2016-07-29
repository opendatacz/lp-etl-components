package com.linkedpipes.plugin.loader.dcatAp11ToCkanBatch;

import org.junit.Test;
import org.openrdf.rio.RDFFormat;

import com.linkedpipes.etl.component.test.TestEnvironment;
import com.linkedpipes.etl.component.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Klímek Jakub
 */
public class DcatAp11ToCkanBatchTest {

    private static final Logger LOG = LoggerFactory.getLogger(DcatAp11ToCkanBatchTest.class);

    @Test
    public void loadTest() throws Exception {
        final DcatAp11ToCkanBatch component = new DcatAp11ToCkanBatch();
        component.configuration = new DcatAp11ToCkanBatchConfiguration();
        component.configuration.setApiUri("http://xrg11.projekty.ms.mff.cuni.cz:5005/api/3/action");
        component.configuration.setApiKey("64d4c649-173a-4623-ab45-ec17e0ceec30");
        component.configuration.setLoadLanguage("cs");

        try (final TestEnvironment env = TestEnvironment.create(component, TestUtils.getTempDirectory())) {
            TestUtils.load(env.bindSingleGraphDataUnit("Metadata"),
                    TestUtils.fileFromResource("input.ttl"), RDFFormat.TURTLE);

            env.execute();
        } catch (Exception ex) {
            LOG.error("Failure", ex);
        }
    }

}
