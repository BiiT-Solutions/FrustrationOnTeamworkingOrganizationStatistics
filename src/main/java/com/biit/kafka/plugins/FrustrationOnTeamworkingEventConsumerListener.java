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

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.Event;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@EnableKafka
@Configuration
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class FrustrationOnTeamworkingEventConsumerListener extends EventListener {

    public FrustrationOnTeamworkingEventConsumerListener() {
        // Event is sent0 from this own application.
        super();
        FrustrationOnTeamworkingEventsLogger.info(this.getClass(), "Frustration On Teamworking Organization Statistics Plugin loaded!");
    }

    @Override
    @KafkaListener(topicPattern = ".*",
            clientIdPrefix = "#{'${spring.kafka.client.id}'?:T(java.util.UUID).randomUUID().toString()}",
            groupId = "#{'${spring.kafka.group.id}'?:T(java.util.UUID).randomUUID().toString()}",
            containerFactory = "templateEventListenerContainerFactory", autoStartup = "${spring.kafka.enabled:true}")
    public void eventsListener(@Payload(required = false) Event event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(KafkaHeaders.GROUP_ID) String groupId,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        super.eventsListener(event, offset, groupId, key, partition, topic, timeStamp);
    }
}
