package com.biit.kafka.plugins;

import com.biit.drools.form.DroolsForm;
import com.biit.drools.form.DroolsSubmittedForm;
import com.biit.drools.form.provider.DroolsFormProvider;
import com.biit.factmanager.client.SearchParameters;
import com.biit.factmanager.client.provider.ClientFactProvider;
import com.biit.factmanager.dto.FactDTO;
import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.EventCustomProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Controller;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;


@Controller
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class FrustrationOnTeamworkingEventController {
    private static final String FORM_LABEL = "The 5 Frustrations on Teamworking";
    private static final String DROOLS_RESULT_EVENT_TYPE = "DroolsResultForm";
    private static final String DROOLS_APPLICATION = "BaseFormDroolsEngine";

    private final ClientFactProvider clientFactProvider;
    private final String subscribedTopic;

    private FrustrationOnTeamworkingEventController() {
        this.clientFactProvider = null;
        this.subscribedTopic = null;
    }

    @Autowired(required = false)
    public FrustrationOnTeamworkingEventController(FrustrationOnTeamworkingEventConsumerListener eventConsumerListener,
                                                   ClientFactProvider clientFactProvider,
                                                   FrustrationOnTeamworkingEventSender frustrationOnTeamworkingEventSender,
                                                   @Value("${spring.kafka.frustration.topic:}") String subscribedTopic) {
        this.clientFactProvider = clientFactProvider;
        this.subscribedTopic = subscribedTopic;

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
                        final DroolsForm droolsForm = processEvent(event);
                        if (droolsForm != null) {
                            frustrationOnTeamworkingEventSender.sendResultEvents(droolsForm, event.getCreatedBy(),
                                    event.getOrganization(), event.getSessionId());
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


    private DroolsForm processEvent(Event event) {
        try {
            final DroolsSubmittedForm droolsSubmittedForm = ObjectMapperFactory.getObjectMapper().readValue(event.getPayload(), DroolsSubmittedForm.class);
            //Is it a new form??
            if (Objects.equals(droolsSubmittedForm.getTag(), FORM_LABEL)) {
                final DroolsForm droolsForm = DroolsFormProvider.createStructure(droolsSubmittedForm);
                droolsForm.setSubmittedBy(event.getCreatedBy());
                droolsForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_OUTPUT);
                populateOrganizationForms(droolsForm,
                        event.getOrganization() != null ? event.getOrganization() : event.getCustomProperty(EventCustomProperties.ORGANIZATION));
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
                final String elmentCorrected = element.replace(FORM_LABEL, FrustrationOnTeamworkingEventConverter.FORM_OUTPUT);
                organizationSubmittedForm.getFormVariables().computeIfAbsent(elmentCorrected, k -> new HashMap<>());
                variableValues.forEach((variable, value) -> {
                    organizationSubmittedForm.getFormVariables().get(elmentCorrected).computeIfAbsent(variable, v -> 0);
                    //Increment Value
                    organizationSubmittedForm.getFormVariables().get(elmentCorrected).put(variable,
                            (Double.parseDouble(organizationSubmittedForm.getFormVariables().get(elmentCorrected).get(variable).toString()) + (Double) value)
                                    / frustrationFacts.size());
                });
            });
        }

        organizationSubmittedForm.setTag(FrustrationOnTeamworkingEventConverter.FORM_OUTPUT);
        organizationSubmittedForm.setOrganization(organization);
    }
}
