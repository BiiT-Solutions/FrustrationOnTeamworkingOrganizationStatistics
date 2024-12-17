package com.biit.kafka.plugins;

import com.biit.drools.form.DroolsForm;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.EventCustomProperties;
import com.biit.kafka.events.EventSubject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class FrustrationOnTeamworkingEventConverter {

    private static final String DROOLS_RESULT_EVENT_TYPE = "DroolsResultForm";
    public static final String FORM_ORGANIZATION_OUTPUT = "Frustration On Teamworking Organization";
    public static final String FORM_TEAM_OUTPUT = "Frustration On Teamworking Team";

    @Value("${spring.application.name:#{null}}")
    private String applicationName;

    public Event getEvent(DroolsForm droolsForm, String createdBy) {
        final Event event = new Event(droolsForm.getDroolsSubmittedForm());
        event.setCreatedBy(createdBy);
        event.setMessageId(UUID.randomUUID());
        event.setSubject(EventSubject.CREATED.toString());
        event.setContentType(MediaType.APPLICATION_ATOM_XML_VALUE);
        event.setCreatedAt(LocalDateTime.now());
        event.setReplyTo(applicationName);
        event.setTag(FORM_ORGANIZATION_OUTPUT);
        event.setCustomProperty(EventCustomProperties.FACT_TYPE, DROOLS_RESULT_EVENT_TYPE);
        event.setCustomProperty(EventCustomProperties.SOURCE_TAG, FORM_ORGANIZATION_OUTPUT);
        return event;
    }
}
