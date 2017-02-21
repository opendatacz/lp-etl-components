package com.linkedpipes.plugin.loader.dcatAp11ToCkanBatch;

import com.linkedpipes.etl.test.TestEnvironment;
import com.linkedpipes.etl.test.TestUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DcatAp11ToCkanBatchTest {

    private static final Logger LOG = LoggerFactory.getLogger(DcatAp11ToCkanBatchTest.class);

    //@Test
    public void loadTest() throws Exception {
        final DcatAp11ToCkanBatch component = new DcatAp11ToCkanBatch();
        component.configuration = new DcatAp11ToCkanBatchConfiguration();
        component.configuration.setApiUri("http://xrg11.projekty.ms.mff.cuni.cz:5005/api/3/action");
        component.configuration.setApiKey("7dac5d32-8367-4ad7-b2df-350e4703e3d9");
        component.configuration.setLoadLanguage("cs");
        component.configuration.setProfile("http://plugins.etl.linkedpipes.com/resource/l-dcatAp11ToCkanBatch/profiles/CKAN");

        try (final TestEnvironment env = TestEnvironment.create(component, TestUtils.getTempDirectory())) {
            TestUtils.load(env.bindSingleGraphDataUnit("Metadata"),
                    TestUtils.fileFromResource("input.ttl"), RDFFormat.TURTLE);
            TestUtils.load(env.bindSingleGraphDataUnit("Codelists"),
                    TestUtils.fileFromResource("filetypes-skos.ttl"), RDFFormat.TURTLE);

            env.execute();
        } catch (Exception ex) {
            LOG.error("Failure", ex);
        }
    }

}
