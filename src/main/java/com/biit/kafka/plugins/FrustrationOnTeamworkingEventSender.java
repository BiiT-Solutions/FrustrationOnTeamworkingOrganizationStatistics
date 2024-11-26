package com.biit.kafka.plugins;

import com.biit.drools.form.DroolsForm;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.KafkaEventTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class FrustrationOnTeamworkingEventSender {

    @Value("${spring.kafka.nca.send.topic:}")
    private String sendTopic;

    private final KafkaEventTemplate kafkaTemplate;

    private final FrustrationOnTeamworkingEventConverter frustrationOnTeamworkingEventConverter;

    private FrustrationOnTeamworkingEventSender() {
        this.kafkaTemplate = null;
        this.frustrationOnTeamworkingEventConverter = null;
    }

    @Autowired(required = false)
    public FrustrationOnTeamworkingEventSender(KafkaEventTemplate kafkaTemplate, FrustrationOnTeamworkingEventConverter frustrationOnTeamworkingEventConverter) {
        this.kafkaTemplate = kafkaTemplate;
        this.frustrationOnTeamworkingEventConverter = frustrationOnTeamworkingEventConverter;
    }

    public void sendResultEvents(DroolsForm response, String executedBy, String organization, UUID sessionId) {
        if (kafkaTemplate != null && sendTopic != null && !sendTopic.isEmpty() && response != null) {
            FrustrationOnTeamworkingEventsLogger.debug(this.getClass().getName(), "Preparing for sending events for '{}' ...", response.getName());
            //Send the complete form as an event.
            final Event event = frustrationOnTeamworkingEventConverter.getEvent(response, executedBy);
            event.setSessionId(sessionId);
            event.setOrganization(organization);
            kafkaTemplate.send(sendTopic, event);
            FrustrationOnTeamworkingEventsLogger.debug(this.getClass().getName(), "Event with results from '{}' send!", response.getName());
        }
    }
}
