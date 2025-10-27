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
import com.biit.factmanager.client.SearchParameters;
import com.biit.factmanager.client.provider.ClientFactProvider;
import com.biit.factmanager.dto.FactDTO;
import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.EventCustomProperties;
import com.biit.rest.exceptions.NotFoundException;
import com.biit.server.security.model.IAuthenticatedUser;
import com.biit.usermanager.client.providers.TeamManagerClient;
import com.biit.usermanager.client.providers.UserManagerClient;
import com.biit.usermanager.dto.TeamDTO;
import com.biit.usermanager.dto.UserDTO;
import com.biit.usermanager.security.exceptions.UserDoesNotExistException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Controller;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;


@Controller
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class FrustrationOnTeamworkingEventController {
    private static final String FORM_LABEL = "The 5 Frustrations on Teamworking";
    private static final String DROOLS_RESULT_EVENT_TYPE = "DroolsResultForm";
    private static final String DROOLS_APPLICATION = "BaseFormDroolsEngine";

    private final ClientFactProvider clientFactProvider;
    private final String subscribedTopic;

    private final UserManagerClient userManagerClient;
    private final TeamManagerClient teamManagerClient;

    private FrustrationOnTeamworkingEventController(UserManagerClient userManagerClient, TeamManagerClient teamManagerClient) {
        this.userManagerClient = userManagerClient;
        this.teamManagerClient = teamManagerClient;
        this.clientFactProvider = null;
        this.subscribedTopic = null;
    }

    @Autowired(required = false)
    public FrustrationOnTeamworkingEventController(FrustrationOnTeamworkingEventConsumerListener eventConsumerListener,
                                                   ClientFactProvider clientFactProvider,
                                                   FrustrationOnTeamworkingEventSender frustrationOnTeamworkingEventSender,
                                                   @Value("${spring.kafka.frustration.topic:}") String subscribedTopic,
                                                   UserManagerClient userManagerClient, TeamManagerClient teamManagerClient) {
        this.clientFactProvider = clientFactProvider;
        this.subscribedTopic = subscribedTopic;
        this.userManagerClient = userManagerClient;
        this.teamManagerClient = teamManagerClient;

        //Listen to the topic
        if (eventConsumerListener != null) {
            eventConsumerListener.addListener((event, offset, groupId, key, partition, topic, timeStamp) -> {
                try {
                    if (Objects.equals(topic, subscribedTopic) && event != null && event.getCustomProperty(EventCustomProperties.FACT_TYPE) != null
                            && Objects.equals(event.getCustomProperty(EventCustomProperties.FACT_TYPE), DROOLS_RESULT_EVENT_TYPE)
                            && Objects.equals(event.getTag(), FORM_LABEL)) {
                        FrustrationOnTeamworkingEventsLogger.debug(this.getClass(), "Received event '{}' on topic '{}', key '{}', partition '{}' at '{}'",
                                event, topic, groupId, key, partition, LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp),
                                        TimeZone.getDefault().toZoneId()));
                        final DroolsForm organizationDroolsForm = processOrganizationEvent(event);
                        if (organizationDroolsForm != null) {
                            frustrationOnTeamworkingEventSender.sendResultEvents(organizationDroolsForm, event.getCreatedBy(),
                                    event.getOrganization(), event.getSessionId(), event.getUnit());
                        }


                        try {
                            //Gets the team from the user who has sent the event.
                            final IAuthenticatedUser user = userManagerClient.findByUsername(event.getCreatedBy()).orElseThrow(
                                    () -> new UserDoesNotExistException("No user with username '" + event.getCreatedBy() + "'."));

                            final Collection<TeamDTO> teams = this.teamManagerClient.findByUser(UUID.fromString(user.getUID()));
                            if (teams == null || teams.isEmpty()) {
                                throw new NotFoundException("No teams found for user '" + user.getUsername() + "'");
                            }
                            //We assume that is the first team.
                            final TeamDTO chosenTeam = teams.iterator().next();
                            final DroolsForm teamDroolsForm = processTeamEvent(event, chosenTeam);
                            if (teamDroolsForm != null) {
                                frustrationOnTeamworkingEventSender.sendResultEvents(teamDroolsForm, event.getCreatedBy(),
                                        chosenTeam.getOrganization() != null ? chosenTeam.getOrganization().getName() : event.getOrganization(),
                                        event.getSessionId(), chosenTeam.getName());
                            }
                        } catch (Exception e) {
                            FrustrationOnTeamworkingEventsLogger.errorMessage(this.getClass(), e);
                        }
                    } else {
                        FrustrationOnTeamworkingEventsLogger.debug(this.getClass(), "Ignoring event topic '" + topic + "'.");
                    }
                } catch (Exception e) {
                    FrustrationOnTeamworkingEventsLogger.errorMessage(this.getClass(), e);
                }
            });
        }
    }


    private DroolsForm processOrganizationEvent(Event event) {
        try {
            final DroolsSubmittedForm droolsSubmittedForm = ObjectMapperFactory.getObjectMapper().readValue(event.getPayload(), DroolsSubmittedForm.class);
            //Is it a new form??
            if (droolsSubmittedForm != null && Objects.equals(droolsSubmittedForm.getTag(), FORM_LABEL)) {
                final DroolsForm droolsForm = DroolsFormProvider.createStructure(droolsSubmittedForm);
                droolsForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_ORGANIZATION_OUTPUT);
                droolsForm.setLabel(FrustrationOnTeamworkingEventConverter.FORM_ORGANIZATION_OUTPUT);
                droolsForm.setSubmittedBy(event.getCreatedBy());
                droolsForm.setSubmittedAt(event.getCreatedAt());
                populateOrganizationForms(droolsForm, event.getOrganization() != null ? event.getOrganization()
                        : event.getCustomProperty(EventCustomProperties.ORGANIZATION));
                return droolsForm;
            }
        } catch (JsonProcessingException e) {
            FrustrationOnTeamworkingEventsLogger.debug(this.getClass(), "Received event is not a NCA FormResult!");
        } catch (Exception e) {
            FrustrationOnTeamworkingEventsLogger.errorMessage(this.getClass(), e);
        }
        return null;
    }


    protected void populateOrganizationForms(DroolsForm droolsForm, String organization) throws JsonProcessingException {
        if (droolsForm == null) {
            return;
        }
        final DroolsSubmittedForm organizationSubmittedForm = ((DroolsSubmittedForm) droolsForm.getDroolsSubmittedForm());
        organizationSubmittedForm.setFormVariables(new HashMap<>());

        //Gets all forms from the organization.
        final Map<SearchParameters, Object> filter = new HashMap<>();
        filter.putIfAbsent(SearchParameters.APPLICATION, DROOLS_APPLICATION);
        filter.putIfAbsent(SearchParameters.ORGANIZATION, organization);
        filter.putIfAbsent(SearchParameters.LATEST_BY_USER, "true");
        filter.putIfAbsent(SearchParameters.GROUP, subscribedTopic);
        filter.putIfAbsent(SearchParameters.ELEMENT_NAME, FORM_LABEL);
        filter.putIfAbsent(SearchParameters.FACT_TYPE, DROOLS_RESULT_EVENT_TYPE);
        final List<FactDTO> frustrationFacts = clientFactProvider.get(filter);

        for (FactDTO frustrationEvent : frustrationFacts) {
            //Read the variables and populate a submittedForm
            final DroolsSubmittedForm organizationFrustrationForm = ObjectMapperFactory.getObjectMapper().readValue(frustrationEvent.getValue(),
                    DroolsSubmittedForm.class);

            organizationFrustrationForm.getFormVariables().forEach((element, variableValues) -> {
                //Correct form name.
                final String elementCorrected = element.replace(FORM_LABEL, FrustrationOnTeamworkingEventConverter.FORM_ORGANIZATION_OUTPUT);
                aggregateValues(organizationSubmittedForm, frustrationFacts, variableValues, elementCorrected);
            });
        }

        organizationSubmittedForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_ORGANIZATION_OUTPUT);
        organizationSubmittedForm.setOrganization(organization);
    }

    private DroolsForm processTeamEvent(Event event, TeamDTO team) {
        try {
            final DroolsSubmittedForm droolsSubmittedForm = ObjectMapperFactory.getObjectMapper().readValue(event.getPayload(), DroolsSubmittedForm.class);
            //Is it a new form??
            if (droolsSubmittedForm != null && Objects.equals(droolsSubmittedForm.getTag(), FORM_LABEL)) {
                final DroolsForm droolsForm = DroolsFormProvider.createStructure(droolsSubmittedForm);
                droolsForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_TEAM_OUTPUT);
                droolsForm.setLabel(FrustrationOnTeamworkingEventConverter.FORM_TEAM_OUTPUT);
                droolsForm.setSubmittedBy(event.getCreatedBy());
                droolsForm.setSubmittedAt(event.getCreatedAt());
                populateTeamForms(droolsForm, team);
                return droolsForm;
            }
        } catch (JsonProcessingException e) {
            FrustrationOnTeamworkingEventsLogger.debug(this.getClass(), "Received event is not a NCA FormResult!");
        } catch (Exception e) {
            FrustrationOnTeamworkingEventsLogger.errorMessage(this.getClass(), e);
        }
        return null;
    }


    protected void populateTeamForms(DroolsForm droolsForm, TeamDTO team) throws JsonProcessingException {
        final DroolsSubmittedForm teamSubmittedForm = ((DroolsSubmittedForm) droolsForm.getDroolsSubmittedForm());
        teamSubmittedForm.setFormVariables(new HashMap<>());

        final Collection<UserDTO> members = userManagerClient.findByTeam(team.getId());

        //Gets all forms from the team.
        final Map<SearchParameters, Object> filter = new HashMap<>();
        filter.putIfAbsent(SearchParameters.APPLICATION, DROOLS_APPLICATION);
        filter.putIfAbsent(SearchParameters.ORGANIZATION, team.getOrganization() != null ? team.getOrganization().getName() : null);
        filter.putIfAbsent(SearchParameters.LATEST_BY_USER, "true");
        filter.putIfAbsent(SearchParameters.GROUP, subscribedTopic);
        filter.putIfAbsent(SearchParameters.ELEMENT_NAME, FORM_LABEL);
        filter.putIfAbsent(SearchParameters.FACT_TYPE, DROOLS_RESULT_EVENT_TYPE);
        filter.putIfAbsent(SearchParameters.CREATED_BY, members.stream().map(IAuthenticatedUser::getUsername).toList());
        final List<FactDTO> frustrationFacts = clientFactProvider.get(filter);

        for (FactDTO frustrationEvent : frustrationFacts) {
            //Read the variables and populate a submittedForm
            final DroolsSubmittedForm organizationFrustrationForm = ObjectMapperFactory.getObjectMapper().readValue(frustrationEvent.getValue(),
                    DroolsSubmittedForm.class);

            organizationFrustrationForm.getFormVariables().forEach((element, variableValues) -> {
                //Correct form name.
                final String elementCorrected = element.replace(FORM_LABEL, FrustrationOnTeamworkingEventConverter.FORM_TEAM_OUTPUT);
                aggregateValues(teamSubmittedForm, frustrationFacts, variableValues, elementCorrected);
            });
        }

        teamSubmittedForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_TEAM_OUTPUT);
        teamSubmittedForm.setOrganization(team.getOrganization() != null ? team.getOrganization().getName() : null);
    }


    private void aggregateValues(DroolsSubmittedForm teamSubmittedForm, List<FactDTO> frustrationFacts, Map<String, Object> variableValues,
                                 String elementCorrected) {
        teamSubmittedForm.getFormVariables().computeIfAbsent(elementCorrected, k -> new HashMap<>());
        variableValues.forEach((variable, value) -> {
            teamSubmittedForm.getFormVariables().get(elementCorrected).computeIfAbsent(variable, v -> 0);
            //Increment Value
            teamSubmittedForm.getFormVariables().get(elementCorrected).put(variable,
                    Double.parseDouble(teamSubmittedForm.getFormVariables().get(elementCorrected).get(variable).toString())
                            + (((Double) value) / frustrationFacts.size()));
        });
    }
}
