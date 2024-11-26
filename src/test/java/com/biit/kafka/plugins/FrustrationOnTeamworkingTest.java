package com.biit.kafka.plugins;

import com.biit.drools.form.DroolsForm;
import com.biit.drools.form.DroolsSubmittedForm;
import com.biit.drools.form.provider.DroolsFormProvider;
import com.biit.utils.file.FileReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;

@SpringBootTest
@Test(groups = "frustrationOnTeamworking")
public class FrustrationOnTeamworkingTest extends AbstractTestNGSpringContextTests {

    private static final String DROOLS_FORM_FILE_PATH = "drools/The 5 Frustrations on Teamworking.json";
    private static final String ORGANIZATION = null;

    @Autowired
    private FrustrationOnTeamworkingEventController frustrationOnTeamworkingEventController;

    @BeforeClass
    public void check() {
        Assert.assertNotNull(frustrationOnTeamworkingEventController);
    }


    @Test
    public void createOrganizationForm() throws FileNotFoundException, JsonProcessingException {
        final DroolsSubmittedForm droolsSubmittedForm = DroolsSubmittedForm.getFromJson(FileReader.getResource(DROOLS_FORM_FILE_PATH, StandardCharsets.UTF_8));

        final DroolsForm droolsForm = DroolsFormProvider.createStructure(droolsSubmittedForm);
        droolsForm.setSubmittedBy(droolsSubmittedForm.getSubmittedBy());
        droolsForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_OUTPUT);


        frustrationOnTeamworkingEventController.populateOrganizationForms(droolsForm, ORGANIZATION);

        Assert.assertTrue(!((DroolsSubmittedForm) droolsForm.getDroolsSubmittedForm()).getFormVariables().isEmpty());
    }
}
