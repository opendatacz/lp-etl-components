package com.linkedpipes.plugin.exec.ckanPurger;

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
public class CkanPurgerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CkanPurgerTest.class);

    @Test
    public void loadTest() throws Exception {
        final CkanPurger component = new CkanPurger();
        component.configuration = new CkanPurgerConfiguration();
        component.configuration.setApiUri("http://xrg11.projekty.ms.mff.cuni.cz:5005/api/3/action");
        component.configuration.setApiKey("bc3d8fc2-9dc5-4f0f-a991-054c9149ba3c");

        try (final TestEnvironment env = TestEnvironment.create(component, TestUtils.getTempDirectory())) {
            env.execute();
        } catch (Exception ex) {
            LOG.error("Failure", ex);
        }
    }

}
