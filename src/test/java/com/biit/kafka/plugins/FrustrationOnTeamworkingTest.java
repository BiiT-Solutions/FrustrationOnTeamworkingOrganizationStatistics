package com.biit.kafka.plugins;

/*-
 * #%L
 * Frustration on Teamworking Organization Statistics Generator
 * %%
 * Copyright (C) 2024 - 2025 BiiT Sourcing Solutions S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
        droolsForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_ORGANIZATION_OUTPUT);


        frustrationOnTeamworkingEventController.populateOrganizationForms(droolsForm, ORGANIZATION);

        Assert.assertTrue(!((DroolsSubmittedForm) droolsForm.getDroolsSubmittedForm()).getFormVariables().isEmpty());
    }
}
