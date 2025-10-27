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

    @Value("${spring.kafka.frustration.send.topic:}")
    private String sendTopic;

    private final KafkaEventTemplate kafkaTemplate;

    private final FrustrationOnTeamworkingEventConverter frustrationOnTeamworkingEventConverter;

    private FrustrationOnTeamworkingEventSender() {
        this.kafkaTemplate = null;
        this.frustrationOnTeamworkingEventConverter = null;
    }

    @Autowired(required = false)
    public FrustrationOnTeamworkingEventSender(KafkaEventTemplate kafkaTemplate,
                                               FrustrationOnTeamworkingEventConverter frustrationOnTeamworkingEventConverter) {
        this.kafkaTemplate = kafkaTemplate;
        this.frustrationOnTeamworkingEventConverter = frustrationOnTeamworkingEventConverter;
    }

    public void sendResultEvents(DroolsForm response, String executedBy, String organization, UUID sessionId, String unit) {
        if (kafkaTemplate != null && sendTopic != null && !sendTopic.isEmpty() && response != null) {
            FrustrationOnTeamworkingEventsLogger.debug(this.getClass().getName(), "Preparing for sending events for '{}' ...", response.getName());
            //Send the complete form as an event.
            final Event event = frustrationOnTeamworkingEventConverter.getEvent(response, executedBy, sessionId);
            event.setOrganization(organization);
            event.setUnit(unit);
            kafkaTemplate.send(sendTopic, event);
            FrustrationOnTeamworkingEventsLogger.debug(this.getClass().getName(), "Event with results from '{}' send!", response.getName());
        }
    }
}
